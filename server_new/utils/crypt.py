import datetime

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from pydantic import BaseModel


class CryptPassword:
    def __init__(self):
        self.ph = PasswordHasher()

    def hash_password(self, password):
        return self.ph.hash(password)

    def verify_password(self, hashed_password, password):
        try:
            return self.ph.verify(hashed_password, password)
        except VerifyMismatchError:
            return False


class TokenPayload(BaseModel):
    sub: str
    exp: datetime.datetime


class TokenManager:
    def __init__(self, secret_key="secret-key", algorithm="HS256", expire_minutes=60):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.expire_minutes = expire_minutes

    def create_access_token(self, subject: str) -> str:
        expire = datetime.datetime.now() + datetime.timedelta(minutes=self.expire_minutes)
        payload = {"sub": subject, "exp": expire}
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def decode_token(self, token: str) -> TokenPayload:
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return TokenPayload(**payload)
        except jwt.ExpiredSignatureError:
            raise Exception("Token expired")
        except jwt.PyJWTError:
            raise Exception("Invalid token")
