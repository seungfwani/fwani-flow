import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from api.auth_dependency import get_current_user
from api.models.api_model import api_response_wrapper, APIResponse
from api.models.auth_model import UserSignupSchema, TokenResponse, UserSchema
from core.database import get_db
from models.user import User
from utils.crypt import CryptPassword, TokenManager

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
)


@router.post("/signup",
             )
@api_response_wrapper
async def create_user(user: UserSignupSchema, db: Session = Depends(get_db), cp: CryptPassword = Depends()):
    user_in_db = db.query(User).filter(User.email == user.email).first()
    if user_in_db:
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed_pw = cp.hash_password(user.password.get_secret_value())
    new_user = User(username=user.name, email=user.email, hashed_password=hashed_pw)
    db.add(new_user)
    db.commit()
    return f"가입 성공: {new_user.username} ({new_user.email})"


@router.post("/login",
             response_model=TokenResponse,
             )
async def login(
        form_data: OAuth2PasswordRequestForm = Depends(),
        db: Session = Depends(get_db),
        cp: CryptPassword = Depends()
):
    user = db.query(User).filter(User.email == form_data.username).first()
    if not user or not cp.verify_password(user.hashed_password, form_data.password):
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 틀렸습니다")

    # JWT 생성
    token_manager = TokenManager()
    access_token = token_manager.create_access_token(str(user.id))
    return TokenResponse(access_token=access_token)


@router.get("/me",
            response_model=APIResponse[UserSchema], )
@api_response_wrapper
async def get_me(user: User = Depends(get_current_user)):
    return UserSchema(name=user.username, email=user.email)
