from enum import Enum


class FlowSnapshotRole(str, Enum):
    """flow_snapshot.role — 플로당 draft·published 각 최대 1행(부분 유니크 인덱스)."""

    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"
