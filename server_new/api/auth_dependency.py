# from fastapi import Depends, HTTPException
# from fastapi.security import OAuth2PasswordBearer
# from sqlalchemy.orm import Session
#
# from core.database import get_db
# from utils.crypt import TokenManager
#
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")
#
#
# def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
#     token_manager = TokenManager()
#     payload = token_manager.decode_token(token)
#     user_id = payload.sub
#     if user_id is None:
#         raise HTTPException(status_code=401, detail="Invalid token")
#     user = db.query(User).filter(User.id == user_id).first()
#     if not user:
#         raise HTTPException(status_code=401, detail="User not found")
#     return user
#
#
# def get_admin_user(user: User = Depends(get_current_user)):
#     if not user.is_admin:
#         raise HTTPException(status_code=403, detail="관리자만 접근 가능합니다")
#     return user
