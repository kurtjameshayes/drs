import pandas as pd
import json
import os
from typing import Dict, Any, List
from core.base_connector import BaseConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalFileConnector(BaseConnector):
    """
    Connector for local file-based data sources.
    Supports CSV, JSON, Excel, and Parquet files.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = config.get("file_path")
        self.file_type = config.get("file_type", "auto")
        self.encoding = config.get("encoding", "utf-8")
        self.delimiter = config.get("delimiter", ",")
        self.sheet_name = config.get("sheet_name", 0)  # For Excel files
        
        if not self.file_path:
            raise ValueError("file_path is required for local file connector")
    
    def connect(self) -> bool:
        """Validate that file exists and is readable."""
        try:
            if not os.path.exists(self.file_path):
                logger.error(f"File not found: {self.file_path}")
                return False
            
            if not os.access(self.file_path, os.R_OK):
                logger.error(f"File not readable: {self.file_path}")
                return False
            
            self.connected = True
            return True
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """Close connection (no persistent connection needed for files)."""
        self.connected = False
        return True
    
    def validate(self) -> bool:
        """Validate file can be read and parsed."""
        try:
            if not self.connected:
                self.connect()
            
            # Try to read first few rows to validate format
            file_type = self._detect_file_type()
            
            if file_type == "csv":
                pd.read_csv(self.file_path, nrows=1, encoding=self.encoding, 
                           delimiter=self.delimiter)
            elif file_type == "json":
                with open(self.file_path, 'r', encoding=self.encoding) as f:
                    json.load(f)
            elif file_type == "excel":
                pd.read_excel(self.file_path, nrows=1, sheet_name=self.sheet_name)
            elif file_type == "parquet":
                pd.read_parquet(self.file_path)
            else:
                return False
            
            return True
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            return False
    
    def _detect_file_type(self) -> str:
        """Detect file type from extension or configuration."""
        if self.file_type != "auto":
            return self.file_type.lower()
        
        ext = os.path.splitext(self.file_path)[1].lower()
        
        if ext in [".csv", ".tsv"]:
            return "csv"
        elif ext == ".json":
            return "json"
        elif ext in [".xlsx", ".xls"]:
            return "excel"
        elif ext == ".parquet":
            return "parquet"
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
    
    def query(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute query on local file.
        
        Args:
            parameters: Query parameters including:
                - columns: List of columns to retrieve (optional)
                - filters: Dict of column filters (optional)
                - limit: Maximum number of rows (optional)
                - offset: Number of rows to skip (optional)
                
        Returns:
            Dict containing query results and metadata
        """
        if not self.connected:
            self.connect()
        
        # Read file into DataFrame
        df = self._read_file()
        
        # Apply column selection
        columns = parameters.get("columns")
        if columns:
            available_cols = [col for col in columns if col in df.columns]
            if available_cols:
                df = df[available_cols]
        
        # Apply filters
        filters = parameters.get("filters", {})
        for column, value in filters.items():
            if column in df.columns:
                if isinstance(value, dict):
                    # Support for operators like {"$gt": 100}
                    if "$gt" in value:
                        df = df[df[column] > value["$gt"]]
                    if "$lt" in value:
                        df = df[df[column] < value["$lt"]]
                    if "$gte" in value:
                        df = df[df[column] >= value["$gte"]]
                    if "$lte" in value:
                        df = df[df[column] <= value["$lte"]]
                    if "$eq" in value:
                        df = df[df[column] == value["$eq"]]
                    if "$ne" in value:
                        df = df[df[column] != value["$ne"]]
                else:
                    # Simple equality filter
                    df = df[df[column] == value]
        
        # Apply sorting
        sort_by = parameters.get("sort_by")
        if sort_by:
            ascending = parameters.get("ascending", True)
            df = df.sort_values(by=sort_by, ascending=ascending)
        
        # Apply pagination
        offset = parameters.get("offset", 0)
        limit = parameters.get("limit")
        
        if offset > 0:
            df = df.iloc[offset:]
        
        if limit:
            df = df.head(limit)
        
        # Convert to records
        data = df.to_dict(orient="records")
        
        return self.transform({"data": data, "total_rows": len(df)})
    
    def _read_file(self) -> pd.DataFrame:
        """Read file into pandas DataFrame."""
        file_type = self._detect_file_type()
        
        if file_type == "csv":
            if self.file_path.endswith(".tsv"):
                return pd.read_csv(self.file_path, encoding=self.encoding, sep="\t")
            return pd.read_csv(self.file_path, encoding=self.encoding, 
                             delimiter=self.delimiter)
        elif file_type == "json":
            return pd.read_json(self.file_path, encoding=self.encoding)
        elif file_type == "excel":
            return pd.read_excel(self.file_path, sheet_name=self.sheet_name)
        elif file_type == "parquet":
            return pd.read_parquet(self.file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
    def transform(self, data: Any) -> Dict[str, Any]:
        """
        Transform file data to standardized format.
        
        Args:
            data: Dict containing 'data' (list of records) and metadata
            
        Returns:
            Dict containing standardized data with metadata
        """
        records = data.get("data", [])
        
        standardized = {
            "metadata": self._create_metadata(len(records), {}),
            "data": records,
            "schema": {
                "fields": self._infer_schema(records) if records else []
            }
        }
        
        return standardized
    
    def _infer_schema(self, records: List[Dict]) -> List[Dict[str, str]]:
        """Infer schema from data records."""
        if not records:
            return []
        
        sample = records[0]
        fields = []
        
        for key, value in sample.items():
            field_type = "string"
            if isinstance(value, (int, float)):
                field_type = "number"
            elif isinstance(value, bool):
                field_type = "boolean"
            
            fields.append({
                "name": key,
                "type": field_type
            })
        
        return fields
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Get connector capabilities."""
        capabilities = super().get_capabilities()
        capabilities.update({
            "supports_pagination": True,
            "supports_filtering": True,
            "supports_sorting": True,
            "supported_formats": ["CSV", "TSV", "JSON", "Excel", "Parquet"],
            "file_path": self.file_path
        })
        return capabilities
    
    def get_column_info(self) -> List[Dict[str, Any]]:
        """
        Get information about available columns.
        
        Returns:
            List of column information
        """
        try:
            df = self._read_file()
            columns = []
            
            for col in df.columns:
                col_info = {
                    "name": col,
                    "type": str(df[col].dtype),
                    "null_count": int(df[col].isnull().sum()),
                    "unique_count": int(df[col].nunique())
                }
                columns.append(col_info)
            
            return columns
        except Exception as e:
            logger.error(f"Failed to get column info: {str(e)}")
            return []
