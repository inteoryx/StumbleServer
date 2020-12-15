import sqlalchemy, datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class User(Base):
    __tablename__ = "User"
    id = Column(String, primary_key=True)
    createdDate = Column(DateTime, server_default=func.now())

    visits = relationship("Visit", back_populates="user")
    submissions = relationship("Submission", back_populates="user")

class Site(Base):
    __tablename__ = "Site"

    id = Column(String, primary_key=True)
    url = Column(String)
    createdDate = Column(DateTime, server_default=func.now())

    visits = relationship("Visit", back_populates="site")

class Visit(Base):
    __tablename__ = "Visit"

    siteId = Column(String, ForeignKey("Site.id"), primary_key=True)
    userId = Column(String, ForeignKey("User.id"), primary_key=True)
    liked = Column(Boolean, default=False)
    createdDate = Column(DateTime, server_default=func.now())
    updatedDate = Column(DateTime, onupdate=func.now())

    user = relationship("User", back_populates="visits")
    site = relationship("Site", back_populates="visits")

class Submission(Base):
    __tablename__ = "Submission"

    userId = Column(String, ForeignKey("User.id"))
    url = Column(String, primary_key=True)
    createdDate = Column(DateTime, server_default=func.now())
    updatedDate = Column(DateTime, onupdate=func.now())
    status = Column(Integer, default=0)
    reason = Column(String, default="")

    user = relationship("User", back_populates="submissions")

    STATUS = {
        0: "PENDING",
        1: "ACCEPTED",
        2: "REJECTED"
    }

    def get_status(self):
        return f"{self.STATUS[self.status]}{':' if len(self.reason) > 0 else ''}{self.reason}"

    def j(self):
        return {
            "user": self.user.id,
            "url": self.url,
            "status": self.status,
            "reason": self.reason,
            "createdDate": self.createdDate,
            "updatedDate": self.updatedDate
        }