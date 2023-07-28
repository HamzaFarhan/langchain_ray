from langchain_ray.imports import *
from langchain_ray.utils import *
from langchain_ray.remote_utils import *
from ray.util.queue import Queue, Empty
from langchain_ray.driver import redis_kv_store


def get_task_from_kv_store(task_id, kv_store):
    task = kv_store.get(task_id)
    if task is None:
        raise Exception(f"No task entry found for task_id: {task_id}.")
    if type(task) != dict:
        raise Exception(f"Wrong type for task with task_id: {task_id}.")
    if len(task) == 0:
        raise Exception(f"Empty dict for task_id: {task_id}.")
    return task


@ray.remote
def queue_consumer(id_, queue, redis_host="127.0.0.1", redis_port=6379) -> None:
    msg.info(f"queue_consumer RAY RESOURCES: {ray.available_resources()}", spaced=True)
    try:
        kv_store = redis_kv_store.KeyValueStore(redis_host=redis_host, redis_port=redis_port)
        while True:
            msg.info(f"Consumer {id_} going to block on queue.", spaced=True)
            task_data = queue.get(block=True)
            task_id = task_data["task_id"]
            tenant_id = task_data["tenant_id"]
            chain = task_data["chain"]
            chain_data = task_data["chain_data"]
            msg.info(f"Consumer {id_} got chain_data: {chain_data.dict()}.", spaced=True)
            try:
                task = get_task_from_kv_store(task_id, kv_store)
            except Exception as e:
                raise Exception(e)
            task["status"] = "TASK_STATUS_INPROGRESS"
            try:
                kv_store.insert(task_id, task)
                res = chain(chain_data)
                try:
                    task = get_task_from_kv_store(task_id, kv_store)
                except Exception as e:
                    raise Exception(e)
                task["status"] = "TASK_STATUS_FINISHED"
                task["results"] = json.dumps(res)
                msg.info(f"Inserting task_id: {task_id}.", spaced=True)
                kv_store.insert(task_id, task)
                msg.info(f"Inserted task_id: {task_id}.", spaced=True)
            except Exception as e:
                msg.fail(f"Queue Consumer failed with error: {e}", spaced=True)
                try:
                    task = get_task_from_kv_store(task_id, kv_store)
                except Exception as e:
                    raise Exception(e)
                task["status"] = "TASK_STATUS_ERROR"
                task["error"] = f"Queue Consumer failed with error: {e}."
                kv_store.insert(task_id, task)
    except Empty:
        pass


class Ingress:
    def __init__(
        self,
        max_queue_size=10,
        num_task_consumers=1,
        redis_host="127.0.0.1",
        redis_port=6379,
    ):
        msg.info(f"Ingress RAY RESOURCES: {ray.available_resources()}", spaced=True)
        try:
            self.kv_store = redis_kv_store.KeyValueStore(
                redis_host=redis_host, redis_port=redis_port
            )
            self.task_queue = Queue(
                maxsize=max_queue_size, actor_options={"num_cpus": 1, "num_gpus": 0}
            )
            self.task_consumers = []
            for i in range(num_task_consumers):
                self.task_consumers.append(
                    queue_consumer.remote(
                        id_=i,
                        queue=self.task_queue,
                        redis_host=redis_host,
                        redis_port=redis_port,
                    )
                )
            msg.info(
                f"Ingress RAY RESOURCES AFTER QUEUE: {ray.available_resources()}", spaced=True
            )
        except Exception as e:
            msg.fail(f"Ingress init failed with error: {e}", spaced=True)

    async def action(self, data):
        if self.task_queue.full():
            raise Exception("\nTask Queue is Full. Please retry later.\n")
        chain_data = data["chain_data"]
        task_data = {"chain": data["chain"], "chain_data": chain_data}
        try:
            task_id = gen_random_string(16)
            tenant_id = chain_data.tenant_id
            self.kv_store.insert(task_id, {"status": "TASK_STATUS_CREATED"})
            task_data["task_id"] = task_id
            task_data["tenant_id"] = tenant_id
            self.task_queue.put(task_data, block=True)
            return {"task_id": task_id, "tenant_id": tenant_id}
        except Exception as e:
            raise Exception(f"Initiating Task failed with error: {e}")
