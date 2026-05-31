import asyncio
import sys
from pathlib import Path

# Add reconciliation-platform to path so we can import backend modules
sys.path.insert(0, str(Path(__file__).parent / "reconciliation-platform"))

from backend.db.base import AsyncSessionLocal, engine
from backend.api.models.platform_transaction import PlatformTransaction
from backend.ingestion.pipeline import ingest_platform_file
from backend.engine.reconciliation_engine import ReconciliationEngine
from backend.api.models.reconciliation_run import ReconciliationRun
import uuid
from datetime import datetime, timezone


async def test_ingestion():
    print("Testing platform file ingestion...")
    test_file_path = Path("reconciliation-platform") / "test_data" / "platform_sample_small.csv"
    file_bytes = test_file_path.read_bytes()
    file_hash = "test-hash-123"
    filename = "platform_sample_small.csv"
    
    async with AsyncSessionLocal() as db:
        result = await ingest_platform_file(
            file_bytes, file_hash, filename, db, actor="test", correlation_id=uuid.uuid4()
        )
        print(f"Ingestion result: {result}")
        
        # Check if transactions were added
        tx_count = (await db.execute(
            __import__("sqlalchemy").func.count(PlatformTransaction.transaction_id)
        )).scalar_one()
        print(f"Total platform transactions in DB: {tx_count}")


async def test_reconciliation():
    print("\nTesting reconciliation engine...")
    # Create a test reconciliation run first
    async with AsyncSessionLocal() as db:
        run_id = str(uuid.uuid4())
        test_run = ReconciliationRun(
            run_id=run_id,
            triggered_by="test",
            started_at_utc=datetime.now(timezone.utc),
            status="queued",
            idempotency_key="test-recon-run"
        )
        db.add(test_run)
        await db.commit()
        print(f"Created reconciliation run with ID: {run_id}")
        
        # Now run the reconciliation engine
        engine = ReconciliationEngine()
        
        async def progress_callback(pct: float, msg: str):
            print(f"Recon progress: {pct:.1f}% - {msg}")
            
        result = await engine.run(run_id, db, progress_callback)
        print(f"Reconciliation result: {result}")


if __name__ == "__main__":
    asyncio.run(test_ingestion())
    asyncio.run(test_reconciliation())
