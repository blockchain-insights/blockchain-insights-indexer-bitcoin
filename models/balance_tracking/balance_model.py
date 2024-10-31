from sqlalchemy import Column, Integer, BigInteger, String, PrimaryKeyConstraint, Index, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BalanceChange(Base):
    __tablename__   = 'balance_changes'

    address         = Column(String, primary_key=True)
    block_height    = Column(Integer, primary_key=True)
    event           = Column(String)
    balance_delta   = Column(BigInteger)
    block_timestamp = Column(TIMESTAMP)
    
    __table_args__ = (
        PrimaryKeyConstraint('address', 'block_height'),
        Index('idx_balance_addr', 'balance_delta', 'address'),
        Index('idx_timestamp_balance_addr', 'block_timestamp', 'balance_delta', 'address'),
        Index('idx_block_balance_addr', 'block_height', 'balance_delta', 'address'),
        Index('idx_balance_changes_block_height', 'block_height')
    )

    def __repr__(self):
        return f"<BalanceChange(address='{self.address}', block={self.block_height}, delta={self.balance_delta})>"


class Block(Base):
    __tablename__   = 'blocks'
    
    block_height     = Column(Integer, primary_key=True)
    timestamp        = Column(TIMESTAMP)
    
    __table_args__ = (
        PrimaryKeyConstraint('block_height'),
        Index('idx_timestamp', 'timestamp'),
    )
