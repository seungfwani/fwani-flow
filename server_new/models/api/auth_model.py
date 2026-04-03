from pydantic import BaseModel, Field, SecretStr, EmailStr


class UserSignupSchema(BaseModel):
    name: str = Field(..., description="user Name", examples=["Fwani"])
    email: EmailStr = Field(..., description="user email", examples=["seungfwani@gmail.com"])
    password: SecretStr = Field(..., description="password", examples=["test"])


class UserSchema(BaseModel):
    name: str = Field(..., description="user Name", examples=["Fwani"])
    email: str = Field(..., description="user email", examples=["seungfwani@gmail.com"])


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
