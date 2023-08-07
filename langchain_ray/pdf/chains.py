# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/pdf/01_pdf_chains.ipynb.

# %% auto 0
__all__ = ['pdf_to_docs_chain', 'add_cats_to_docs_chain']

# %% ../../nbs/pdf/01_pdf_chains.ipynb 2
from ..imports import *
from ..chains import *
from ..utils import *
from .utils import *

# %% ../../nbs/pdf/01_pdf_chains.ipynb 4
def pdf_to_docs_chain(
    splitter=None,
    chunk_size=200,
    chunk_overlap=20,
    separators=["\n\n", "\n", "(?<=\. )", " ", ""],
    add_start_index=True,
    proc=True,
    input_variables=["path"],
    output_variables=["docs"],
    verbose=False,
):
    """Chain that returns a list of `Documents` extracted from a PDF path.
    The path can be a single PDF path or a list of paths or a directory path."""
    return transform_chain(
        pdf_to_docs,
        transform_kwargs=dict(
            splitter=splitter,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=separators,
            add_start_index=add_start_index,
            proc=proc,
        ),
        vars_kwargs_mapping={input_variables[0]: "path"},
        input_variables=input_variables,
        output_variables=output_variables,
        verbose=verbose,
    )


def add_cats_to_docs_chain(
    cats_model,
    input_variables=["docs"],
    output_variables=["cat_docs"],
    verbose=False,
):
    "Chain that adds the categories to a list of `Documents` using `cats_model`."
    return transform_chain(
        add_cats_to_docs,
        transform_kwargs=dict(cats_model=cats_model),
        input_variables=input_variables,
        output_variables=output_variables,
        vars_kwargs_mapping={input_variables[0]: "docs"},
        verbose=verbose,
    )
