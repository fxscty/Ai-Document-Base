from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    relationship,
)


class Base(DeclarativeBase, MappedAsDataclass):
    pass


class DocumentStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"


class User(Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255))

    id: Mapped[int] = mapped_column(Identity(), primary_key=True, init=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), init=False
    )

    documents: Mapped[list[Document]] = relationship(
        back_populates="user", cascade="all, delete-orphan", init=False
    )
    chats: Mapped[list[Chat]] = relationship(
        back_populates="user", cascade="all, delete-orphan", init=False
    )


class Document(Base):
    __tablename__ = "documents"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    filename: Mapped[str] = mapped_column(String(255))
    file_path: Mapped[str] = mapped_column(String(512))

    id: Mapped[int] = mapped_column(Identity(), primary_key=True, init=False)
    status: Mapped[DocumentStatus] = mapped_column(
        Enum(DocumentStatus), default=DocumentStatus.PENDING, init=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), init=False
    )

    user: Mapped[User] = relationship(back_populates="documents", init=False)
    chunks: Mapped[list[Chunk]] = relationship(
        back_populates="document", cascade="all, delete-orphan", init=False
    )


class Chunk(Base):
    __tablename__ = "chunks"

    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id", ondelete="CASCADE"), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(Text)
    # 1536 — для OpenAI text-embedding-3-small (или 1024 для BGE-M3)
    embedding: Mapped[list[float]] = mapped_column(Vector(1536))

    id: Mapped[int] = mapped_column(Identity(), primary_key=True, init=False)

    document: Mapped[Document] = relationship(back_populates="chunks", init=False)

    # HNSW-индекс для быстрого косинусного поиска векторов
    __table_args__ = (
        Index(
            "ix_chunks_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )


class Chat(Base):
    __tablename__ = "chats"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    title: Mapped[str] = mapped_column(String(255), default="New Chat")

    id: Mapped[int] = mapped_column(Identity(), primary_key=True, init=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), init=False
    )

    user: Mapped[User] = relationship(back_populates="chats", init=False)
    messages: Mapped[list[Message]] = relationship(
        back_populates="chat", cascade="all, delete-orphan", init=False
    )


class Message(Base):
    __tablename__ = "messages"

    chat_id: Mapped[int] = mapped_column(ForeignKey("chats.id", ondelete="CASCADE"), index=True)
    role: Mapped[MessageRole] = mapped_column(Enum(MessageRole))
    content: Mapped[str] = mapped_column(Text)

    id: Mapped[int] = mapped_column(Identity(), primary_key=True, init=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), init=False
    )

    chat: Mapped[Chat] = relationship(back_populates="messages", init=False)