from sqlalchemy import Column, Integer, BigInteger, String, PrimaryKeyConstraint, Index, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BalanceChange(Base):
    __tablename__ = 'balance_changes'

    address = Column(String, nullable=False)
    block_height = Column(Integer, nullable=False)
    event = Column(String)
    balance_delta = Column(BigInteger, nullable=False)
    block_timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('address', 'block_height'),
        # Indexes matching init.sql
        Index('idx_balance_changes_block_height', 'block_height'),
        Index('idx_balance_changes_addr_time', 'address', 'block_timestamp', postgresql_ops={'block_timestamp': 'DESC'})
    )

    def __repr__(self):
        return f"<BalanceChange(address='{self.address}', block={self.block_height}, delta={self.balance_delta})>"


class Block(Base):
    __tablename__ = 'blocks'

    block_height = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP(timezone=True))

    __table_args__ = (
        Index('idx_timestamp', 'timestamp'),
    )

    def __repr__(self):
        return f"<Block(height={self.block_height}, timestamp={self.timestamp})>"


class BalanceAddressBlock(Base):
    __tablename__ = 'balance_address_blocks'

    address = Column(String, nullable=False)
    block_height = Column(Integer, nullable=False)
    balance = Column(BigInteger, nullable=False)
    block_timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('address', 'block_height'),
        # Indexes matching init.sql
        Index('idx_bab_addr_height_lookup', 'address', 'block_height', postgresql_ops={'block_height': 'DESC'}),
        Index('idx_bab_height_balance_top', 'block_height', 'balance',
              postgresql_ops={'block_height': 'DESC', 'balance': 'DESC'},
              postgresql_include=['address']),
        Index('idx_bab_height_balance_range', 'block_height', 'balance',
              postgresql_ops={'block_height': 'DESC'},
              postgresql_include=['address']),
        Index('idx_bab_timestamp_balance', 'block_timestamp', 'balance',
              postgresql_ops={'block_timestamp': 'DESC'},
              postgresql_include=['address', 'block_height']),
        Index('idx_bab_addr_time', 'address', 'block_timestamp',
              postgresql_ops={'block_timestamp': 'DESC'},
              postgresql_include=['balance', 'block_height'])
    )

    def __repr__(self):
        return f"<BalanceAddressBlock(address='{self.address}', block={self.block_height}, balance={self.balance})>"