from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Document(BaseModel):
    id: str
    title: str
    url: str
    date_published: str
    summary: str
    category: str
    hash: str
    created_at: str


class File(BaseModel):
    id: str
    document_id: str
    file_url: str
    file_path: str
    file_type: str  # "pdf"
    pages: int


class Table(BaseModel):
    id: str
    document_id: str
    source_file_id: str
    table_json: List[List[str]]
    n_rows: int
    n_cols: int
