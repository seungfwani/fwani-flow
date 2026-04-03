from sqlalchemy import Column, String

from core.database import BaseDB


class KeycloakUserEntity(BaseDB):
    __tablename__ = "user_entity"
    __table_args__ = {
        "schema": "keycloak",
        "info": {"skip_autogenerate": True},  # alembic 제외
    }

    id = Column(String, primary_key=True)
    username = Column(String)
