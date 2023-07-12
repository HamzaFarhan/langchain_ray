# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/pdf/01_pdf_ner.ipynb.

# %% auto 0
__all__ = ['load_ner_model', 'load_job_model', 'proc_ners', 'job_ner', 'edu_ner', 'work_ner', 'docs_to_ners', 'add_ners_to_docs']

# %% ../../nbs/pdf/01_pdf_ner.ipynb 2
from ..imports import *
from ..chains import *
from ..utils import *
from .utils import *


# %% ../../nbs/pdf/01_pdf_ner.ipynb 4
def load_ner_model(model_name="tner/deberta-v3-large-ontonotes5", device="cpu"):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForTokenClassification.from_pretrained(model_name)
    return pipeline(
        "ner",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy="simple",
        device=device,
    )


def load_job_model(model_name="ismail-lucifer011/autotrain-job_all-903929564", device="cpu"):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForTokenClassification.from_pretrained(model_name)
    return pipeline(
        "ner",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy="simple",
        device=device,
    )


def proc_ners(ners, ner_dict={"institute": "", "date": ""}, thresh=3):
    ner_dict2 = copy.deepcopy(ner_dict)
    org_key = "institute" if "institute" in ner_dict2 else "company"
    mapper = {
        "ORG": org_key,
        "FAC": org_key,
        "GPE": org_key,
        "LOC": org_key,
        "Job": "role",
        "WORK_OF_ART": "degree",
        "DATE": "date",
    }
    ner_dicts = []
    for ner in ners:
        if len(ner) == 0:
            ner_dicts.append({})
            continue
        try:
            for d in ner:
                eg = d["entity_group"]
                w = " " + d["word"].strip()
                k = mapper.get(eg, None)
                if k is not None and ner_dict2.get(k, None) is not None and not w.startswith("##"):
                    ner_dict2[k] = (ner_dict2[k] + w).strip()
            res = {k: v for k, v in ner_dict2.items() if len(v) > thresh}
            if res.get(org_key, None) is not None:
                ner_dicts.append(res)
            else:
                ner_dicts.append({})
            ner_dict2 = copy.deepcopy(ner_dict)
        except Exception as e:
            msg.fail(f"proc_ners failed with error: {e}", spaced=True)
            ner_dicts.append({})
            ner_dict2 = copy.deepcopy(ner_dict)
    return ner_dicts


def job_ner(docs, e_ner, j_ner):
    return j_ner(docs), e_ner(docs)


def edu_ner(docs, e_ner, ner_dict={"institute": "", "date": ""}):
    ners = e_ner(docs)
    return proc_ners(ners, ner_dict)


def work_ner(docs, e_ner, j_ner, ner_dict={"company": "", "date": ""}):
    ner1, ner2 = job_ner(docs, e_ner, j_ner)
    ners = [n1 + n2 for n1, n2 in zip(ner1, ner2)]
    return proc_ners(ners, ner_dict)


def docs_to_ners(docs, e_ner, j_ner):
    ners = [{}] * len(docs)
    work_docs = np.array(
        [
            [i, doc.page_content]
            for i, doc in enumerate(docs)
            if doc.metadata.get("category", None) == "Work Experience"
        ]
    )
    work_docs_idx = work_docs[:, 0].astype(int)
    work_docs = work_docs[:, 1].tolist()
    try:
        work_ners = work_ner(work_docs, e_ner, j_ner)
    except Exception as e:
        msg.fail(f"work_ner failed with error: {e}", spaced=True)
        work_ners = [{}] * len(work_docs)
    for i, doc in enumerate(work_docs_idx):
        ners[doc] = work_ners[i]
    edu_docs = np.array(
        [
            [i, doc.page_content]
            for i, doc in enumerate(docs)
            if doc.metadata.get("category", None) == "Education"
        ]
    )
    edu_docs_idx = edu_docs[:, 0].astype(int)
    edu_docs = edu_docs[:, 1].tolist()
    try:
        edu_ners = edu_ner(edu_docs, e_ner)
    except Exception as e:
        msg.fail(f"edu_ner failed with error: {e}", spaced=True)
        edu_ners = [{}] * len(edu_docs)
    for i, doc in enumerate(edu_docs_idx):
        ners[doc] = edu_ners[i]
    return ners

def add_ners_to_docs(docs, e_ner, j_ner, key="ner"):
    fn = partial(docs_to_ners, e_ner=e_ner, j_ner=j_ner)
    return add_docs_metadata(docs, fn, key)
