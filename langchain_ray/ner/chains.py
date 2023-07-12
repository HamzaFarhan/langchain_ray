# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/ner/01_ner_chains.ipynb.

# %% auto 0
__all__ = ['add_ners_to_docs_chain']

# %% ../../nbs/ner/01_ner_chains.ipynb 2
from ..imports import *
from ..chains import *
from ..utils import *
from ..pdf.utils import *
from ..pdf.chains import *
from .utils import *


# %% ../../nbs/ner/01_ner_chains.ipynb 4
def add_ners_to_docs_chain(
    e_ner,
    j_ner,
    input_variables=["docs"],
    output_variables=["ner_docs"],
    verbose=False,
):
    "Chain that adds the NERs to a list of `Documents` usung `e_ner` and `j_ner`."
    return transform_chain(
        add_ners_to_docs,
        transform_kwargs=dict(e_ner=e_ner, j_ner=j_ner),
        input_variables=input_variables,
        output_variables=output_variables,
        vars_kwargs_mapping={input_variables[0]: "docs"},
        verbose=verbose,
    )
