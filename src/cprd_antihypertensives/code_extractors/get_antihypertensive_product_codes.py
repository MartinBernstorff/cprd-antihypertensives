from typing import Literal, Union

from cprd_antihypertensives.cprd.functions.MedicalDictionary import (
    MedicalDictionaryRiskPrediction,
)
from cprd_antihypertensives.filters.diagnoses.get_diagnoses_matching_codes import Codes
from cprd_antihypertensives.globals import PROJECT_ROOT
from cprd_antihypertensives.utils.load_config import load_config


def get_codes(
    term: Union[str, list[str]],
    output_type: Literal["disease", "measurement", "procedure", "medication"],
    merge: bool = False,
) -> Codes:
    file_paths = load_config(
        config_path=PROJECT_ROOT / "application" / "config" / "config.yaml"
    )["file_path"]

    medical_dict = MedicalDictionaryRiskPrediction(file_paths)

    term = medical_dict.findItem(term)

    if output_type == "medication":
        codes = medical_dict.queryMedication(term, merge=merge)

    if output_type == "procedure":
        codes = medical_dict.queryProcedure(term, merge=merge)

    if output_type == "measurement":
        codes = medical_dict.queryMeasurement(term, merge=merge)

    if output_type == "disease":
        codes = medical_dict.queryDisease(term, merge=merge)

    return codes["merged"] if merge else codes[term[0]]  # type: ignore
