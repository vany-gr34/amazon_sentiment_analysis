from typing import List, Optional

from pydantic import BaseModel, field_validator


class PredictRequest(BaseModel):
    text: str

    @field_validator("text")
    @classmethod
    def text_must_not_be_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("text must not be empty")
        return value


class PredictResponse(BaseModel):
    text: str
    cleaned_text: str
    prediction: str
    class_id: int
    confidence: Optional[float] = None
    model_name: str
    model_stage: str


class BatchItem(BaseModel):
    id: str
    text: str

    @field_validator("text")
    @classmethod
    def text_must_not_be_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("text must not be empty")
        return value


class BatchRequest(BaseModel):
    items: List[BatchItem]


class BatchPrediction(BaseModel):
    id: str
    text: str
    cleaned_text: Optional[str] = None
    prediction: str
    class_id: int
    confidence: Optional[float] = None
    error: Optional[str] = None


class BatchResponse(BaseModel):
    predictions: List[BatchPrediction]
    total: int
    failed: int


class ReloadResponse(BaseModel):
    success: bool
    model_loaded: bool
    vectorizer_loaded: bool
    message: str


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    vectorizer_loaded: bool
    mlflow_tracking_uri: str
    mlflow_s3_endpoint_url: str
    model_name: str
    model_stage: str
    model_uri: str