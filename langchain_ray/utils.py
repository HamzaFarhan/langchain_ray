# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/00_utils.ipynb.

# %% auto 0
__all__ = ['is_doc', 'list_or_array', 'is_nested_list', 'nested_list', 'unnest_list', 'cid_to_char', 'process_text',
           'proc_doc_text', 'bold_text', 'print_doc', 'docs_to_json', 'add_docs_metadata', 'add_str_to_docs']

# %% ../nbs/00_utils.ipynb 2
from dreamai.imports import *
from .imports import *
from .remote_utils import *

# %% ../nbs/00_utils.ipynb 4
def is_doc(x):
    return isinstance(x, Document)


def list_or_array(x):
    return is_list(x) or is_array(x)


def is_nested_list(x):
    return list_or_array(x) and list_or_array(x[0])


def nested_list(x):
    is_nested = is_nested_list(x)
    if not list_or_array(x):
        x = [x]
    if not list_or_array(x[0]):
        x = [x]
    return x, is_nested


def unnest_list(x):
    if is_list(x) and is_list(x[0]) and len(x) == 1:
        x = x[0]
    return x


def cid_to_char(cidx: str):
    try:
        return chr(int(re.findall(r"\(cid\:(\d+)\)", cidx)[0]) + 29)
    except:
        return cidx


def process_text(text: str):
    txt = text.strip()
    txt = demoji.replace(txt, "")
    txt = clean(
        txt,
        # no_urls=True,
        no_emails=True,
        no_phone_numbers=True,
        no_currency_symbols=True,
        # replace_with_url="",
        replace_with_email="",
        replace_with_phone_number="",
        replace_with_currency_symbol="",
    )
    txt = cid_to_char(txt)
    txt = re.sub("\xa0", " ", txt)
    txt = re.sub(r"\uf0b7", " ", txt)
    txt = re.sub(r"\(cid:\d{0,3}\)", " ", txt)
    txt = re.sub(r"•", "", txt)
    txt = re.sub(r"●", "", txt)
    txt = re.sub(r"▪", "", txt)
    txt = re.sub(r"", "", txt)
    txt = re.sub(r"➢", "", txt)
    txt = re.sub(r"\u2b9a", "", txt)
    txt = re.sub(r"\u201c", "", txt)
    txt = re.sub(r"\u201d", "", txt)
    txt = re.sub(r"\u2013", " ", txt)
    txt = re.sub(r"\u2019", "'", txt)
    txt = re.sub(r"\u2018", "'", txt)
    txt = re.sub(r"\u00f4", " ", txt)
    txt = re.sub(r"\u00f6", "o", txt)
    txt = re.sub(r"\u00e9", "e", txt)
    txt = re.sub(r"\u00e8", "e", txt)
    txt = re.sub(r"\u00e7", " ", txt)
    txt = re.sub(r"\u00a7", "", txt)
    txt = re.sub(r"\u00e3", "a", txt)
    txt = re.sub(r"\uf0a7", "", txt)
    txt = re.sub(r"\uf076", "", txt)
    txt = re.sub(r"\u00ad", "", txt)
    txt = re.sub(r"\u00ab", "", txt)
    txt = re.sub(r"\u00bb", "", txt)
    txt = re.sub(r"\uf02d", "", txt)
    txt = re.sub(r"\uf0fc", "", txt)
    txt = re.sub(r"\uf06e", "", txt)
    txt = re.sub(r"\uf07a", "", txt)
    txt = re.sub(r"\ufb01", "fi", txt)
    txt = re.sub(r"\ufb00", "ff", txt)
    txt = re.sub(r"\uf0d8", "", txt)
    txt = re.sub(r"\u00b7", "", txt)
    txt = re.sub("\t", " ", txt)
    txt = re.sub(" +", " ", txt)
    return txt.strip()


def proc_doc_text(doc):
    doc.page_content = process_text(doc.page_content)
    return doc


def bold_text(text):
    return "\033[1m" + text + "\033[0m"


def print_doc(doc):
    print(f"{bold_text('Page_Content:')} {doc.page_content}\n")
    print(f"{bold_text('Metadata:')} {doc.metadata}\n")


def docs_to_json(
    docs,
    json_folder,
    data={},
    data_key="data",
    with_metadata=True,
    with_content=False,
    indent=None,
):
    if is_list(data):
        data = {data_key: data}
    json_folder, remote_folder = handle_input_path(json_folder)
    os.makedirs(json_folder, exist_ok=True)
    for i, doc in enumerate(flatten_list(docs)):
        doc_dict = {}
        if with_content:
            doc_dict["page_content"] = doc.page_content
        if with_metadata:
            doc_dict["metadata"] = doc.metadata
        for k, v in data.items():
            if is_list(v) and len(v) == len(docs):
                doc_dict[k] = v[i]
        if len(doc_dict) == 0:
            doc_dict = {"page_content": doc.page_content, "metadata": doc.metadata}
        source = Path(doc.metadata["source"])
        json_path = (Path(json_folder) / source.stem).with_suffix(".json")
        if json_path.exists():
            json_path = find_alternate_path(json_path, first_idx=1, verbose=False)

        # print(f"\n\nDOC_DICT: {doc_dict}\n\n")

        with open(json_path, "w") as f:
            json.dump(doc_dict, f, indent=indent)
    if is_bucket(remote_folder):
        bucket_up(json_folder, remote_folder)
    return docs


def add_docs_metadata(docs, fn=None, key="new_meta"):
    if fn is None:
        return docs
    docs, is_nested = nested_list(docs)
    for docs_ in docs:
        fn_res = fn(docs_)
        for doc, res in zip(docs_, fn_res):
            doc.metadata[key] = res
    return docs if is_nested else docs[0]


def add_str_to_docs(docs, str, key="new_meta"):
    fn = lambda x: [str] * len(x)
    return add_docs_metadata(docs, fn=fn, key=key)

