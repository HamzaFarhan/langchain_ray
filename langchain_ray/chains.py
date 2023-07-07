# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/00_chains.ipynb.

# %% auto 0
__all__ = ['chain_fn', 'transform_chain', 'ray_chain_fn', 'ray_chain', 'noop_chain']

# %% ../nbs/00_chains.ipynb 2
from dreamai.imports import *
from .imports import *

# %% ../nbs/00_chains.ipynb 3
def chain_fn(data, tfm, tfm_kwargs={}, input_key="df", output_key="df"):
    return {output_key: tfm(data[input_key], **tfm_kwargs)}


def transform_chain(transform, transform_kwargs={}, input_key="df", output_key="df"):
    return TransformChain(
        input_variables=[input_key],
        output_variables=[output_key],
        transform=partial(
            chain_fn,
            tfm=transform,
            tfm_kwargs=transform_kwargs,
            input_key=input_key,
            output_key=output_key,
        ),
    )


def ray_chain_fn(data, chain, block_size=1500, cuda=True, max_cpus=8):
    df = data[chain.input_keys[0]]
    if not is_df(df):
        res = chain.run(df)
    elif block_size is None or len(df) <= block_size:
        res = chain.run(df)
    else:
        num_blocks = int(np.ceil(len(df) / block_size))
        msg.info(f"Running chain on {num_blocks} blocks.", spaced=True)
        num_cpus = min(ray.available_resources()["CPU"] - 4, max_cpus)
        num_cpus /= num_blocks
        num_gpus = None
        if cuda:
            num_gpus = (ray.available_resources()["GPU"] - 0.25) / num_blocks
            num_cpus = None
        ds = rd.from_pandas(df).repartition(num_blocks)
        res = ds.map_batches(
            lambda x: chain.run(x),
            batch_size=block_size,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            batch_format="pandas",
        ).to_pandas()
    return {chain.output_keys[0]: res}


def ray_chain(chain, block_size=1500, cuda=True):
    tfm = partial(ray_chain_fn, chain=chain, block_size=block_size, cuda=cuda)
    input_variables = chain.input_keys
    output_variables = chain.output_keys
    return TransformChain(
        input_variables=input_variables,
        output_variables=output_variables,
        transform=tfm,
    )


def noop_chain():
    return transform_chain(noop)

