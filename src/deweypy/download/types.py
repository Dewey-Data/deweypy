from __future__ import annotations

from typing import Literal, TypeAlias, TypedDict

APIMethod: TypeAlias = Literal["GET", "POST", "PUT", "DELETE", "PATCH"]


class GetMetadataDict(TypedDict):
    total_files: int
    total_size: int
    total_partitions: int
    partition_type: Literal["DATE", "CATEGORICAL"] | None
    partition_column: str | None
    partition_aggregation: Literal["DAY", "MONTH"] | None
    min_partition_key: str | None
    max_partition_key: str | None


class DownloadItemDict(TypedDict):
    link: str
    partition_key: str | None
    file_name: str
    file_extension: Literal[".csv", ".csv.gz", ".parquet", ".parquet.gz"]
    file_size_bytes: int
    modified_at: str  # ISO-8601 formatted datetime string
    external_id: str


class ZippedDownloadItemDict(DownloadItemDict):
    is_zip_file: Literal[True]


class GetFilesDict(TypedDict):
    download_links: list[DownloadItemDict]
    page: int
    number_of_files_for_page: int
    avg_file_size_for_page: int | float | None
    partition_column: str | None
    total_files: int
    total_pages: int
    total_size: int
    expires_at: str  # ISO-8601 formatted datetime string
    zip_file: ZippedDownloadItemDict | None


class DoiDict(TypedDict):
    doi_type: Literal["canonical", "versioned"]
    doi: str
    citation_apa: str
    dewey_internal_version: str


class GetDescriptionBaseDict(TypedDict):
    dataset_external_id: str
    dataset_name: str
    dataset_slug: str
    dataset_is_primary: bool
    dataset_is_supplementary: bool
    dataset_partner_specific_terms: str
    dataset_docs_url: str
    dataset_citations_url: str
    dataset_support_url: str
    dataset_last_refreshed_at: str  # ISO-8601 formatted datetime string
    dataset_version_timestamp: str  # ISO-8601 formatted datetime string

    partner_external_id: str
    partner_name: str
    partner_slug: str

    folder_external_id: str

    doi_canonical: DoiDict | None
    doi_versioned: DoiDict | None

    last_refreshed_at: str  # ISO-8601 formatted datetime string
    version_timestamp: str  # ISO-8601 formatted datetime string


class GetDescriptionNonFileOnlyDatasetDict(GetDescriptionBaseDict):
    type: Literal["dataset"]
    dataset_is_file_only: Literal[False]

    unload_external_id: str
    unloaded_at: str  # ISO-8601 formatted datetime string


class GetDescriptionFileOnlyDatasetDict(GetDescriptionBaseDict):
    type: Literal["dataset"]
    dataset_is_file_only: Literal[True]


class GetDescriptionCustomizedNonFileOnlyDatasetDict(GetDescriptionBaseDict):
    type: Literal["customized_dataset"]
    dataset_is_file_only: Literal[False]

    unload_external_id: str
    unloaded_at: str  # ISO-8601 formatted datetime string

    customized_external_id: str
    customized_name: str


# NOTE: The above names are a bit verbose, but more descriptive. Here are some
# shorter aliases.
DescribedDatasetRegularDict: TypeAlias = GetDescriptionNonFileOnlyDatasetDict
DescribedDatasetFileOnlyDict: TypeAlias = GetDescriptionFileOnlyDatasetDict
DescribedCustomizedDatasetDict: TypeAlias = (
    GetDescriptionCustomizedNonFileOnlyDatasetDict
)
DescribedDatasetDict: TypeAlias = (
    DescribedDatasetRegularDict
    | DescribedDatasetFileOnlyDict
    | DescribedCustomizedDatasetDict
)
