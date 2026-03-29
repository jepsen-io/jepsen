#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import uuid


DEFAULT_DATABASE = "sendapay_jepsen_bank"
APPLICATION_NAME = "sendapay-jepsen-bank"


def _write_json(payload: dict, *, stream=None) -> None:
    target = stream or sys.stdout
    json.dump(payload, target, sort_keys=True)
    target.write("\n")


def _add_sendapay_root(sendapay_root: str) -> Path:
    root = Path(sendapay_root).resolve()
    if not root.exists():
        raise RuntimeError(f"Sendapay root not found: {root}")
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def _feature_flags(append_only: bool) -> dict[str, bool]:
    return {
        "FEATURE_P2P_APPEND_ONLY_CONFIRM_ENABLED": bool(append_only),
        "FEATURE_WALLET_BALANCE_PROJECTION_READS_ENABLED": bool(append_only),
        "FEATURE_WALLET_AUTHORITATIVE_SPENDABLE_ENABLED": bool(append_only),
    }


def _configure_local_metrics_env() -> None:
    # The benchmark helper runs a single app process per benchmark node; shared
    # cross-process metrics snapshot caching only adds import-time waits here.
    os.environ["CORE_DB_METRICS_SHARED_CACHE_ENABLED"] = "0"
    os.environ["PRECOMPUTE_CORE_METRICS_ON_BOOT"] = "0"


def _build_app_from_state(state: dict):
    sendapay_root = _add_sendapay_root(state["sendapay_root"])
    from scripts.benchmark_common import build_local_benchmark_app

    _configure_local_metrics_env()
    args = SimpleNamespace(
        db_url=state["db_url"],
        pool_size=int(state["pool_size"]),
        max_overflow=int(state["max_overflow"]),
    )
    app = build_local_benchmark_app(
        args,
        default_database=DEFAULT_DATABASE,
        application_name=APPLICATION_NAME,
        feature_flags=_feature_flags(bool(state.get("append_only"))),
    )
    return sendapay_root, app


def _dispose_app(app) -> None:
    try:
        from scripts.benchmark_common import dispose_benchmark_app

        dispose_benchmark_app(app)
    except Exception:
        pass


def _load_state(state_file: str) -> dict:
    return json.loads(Path(state_file).read_text())


def _save_state(state_file: str, payload: dict) -> None:
    path = Path(state_file).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _read_balances(tracked_wallet_ids: dict[str, int]) -> dict[str, int]:
    from app.services.wallet_balance_projection import authoritative_wallet_balance_snapshot

    balances: dict[str, int] = {}
    for label, wallet_id in tracked_wallet_ids.items():
        snapshot = authoritative_wallet_balance_snapshot(int(wallet_id))
        balances[str(label)] = int(snapshot.get("ledger_cents") or 0)
    return balances


def _read_balances_consistent(tracked_wallet_ids: dict[str, int]) -> dict[str, object]:
    from sqlalchemy import text

    from app.extensions import db

    # PostgreSQL's default READ COMMITTED isolation can observe one wallet
    # before a transfer commit and its counterparty after that commit. Jepsen
    # needs one stable snapshot across the whole tracked set.
    db.session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    balances = _read_balances(tracked_wallet_ids)
    return {
        "balances": balances,
        "total_amount_cents": int(sum(int(value) for value in balances.values())),
    }


def _serialize_transaction(txn, *, include_sensitive: bool) -> dict | None:
    if txn is None:
        return None
    return {
        "id": int(txn.id),
        "reference": str(txn.reference or ""),
        "status": str(txn.status or ""),
    }


def _serialize_receipt(txn, *, extra: dict, include_sensitive: bool, user_id: int) -> dict | None:
    if txn is None:
        return None
    return {
        "transaction_id": int(txn.id),
        "channel": str((extra or {}).get("channel") or ""),
        "user_id": int(user_id),
    }


def _serialize_intent(intent) -> dict | None:
    if intent is None:
        return None
    return {
        "id": int(intent.id),
        "status": str(intent.status or ""),
        "reference": str(intent.reference or ""),
    }


def command_init(args: argparse.Namespace) -> int:
    sendapay_root = _add_sendapay_root(args.sendapay_root)
    from scripts.benchmark_common import (
        benchmark_phone,
        build_local_benchmark_app,
        ensure_benchmark_user,
        rebuild_local_database,
    )

    _configure_local_metrics_env()
    schema_file = sendapay_root / "db" / "schema" / "sendapay_schema.sql"
    normalized_db_url = rebuild_local_database(
        str(args.db_url),
        default_database=DEFAULT_DATABASE,
        repo_root=sendapay_root,
        schema_file=schema_file,
        python_executable=sys.executable,
    )

    build_args = SimpleNamespace(
        db_url=normalized_db_url,
        pool_size=int(args.pool_size),
        max_overflow=int(args.max_overflow),
    )
    app = build_local_benchmark_app(
        build_args,
        default_database=DEFAULT_DATABASE,
        application_name=APPLICATION_NAME,
        feature_flags=_feature_flags(bool(args.append_only)),
    )

    try:
        with app.app_context():
            from app.extensions import db
            from app.services.ledger import (
                bootstrap_wallet_balance,
                get_fee_revenue_wallet,
                get_issuance_wallet,
            )
            from app.services.wallets import create_wallet

            issuance_wallet = get_issuance_wallet()
            fee_wallet = get_fee_revenue_wallet()
            transfer_accounts: list[str] = []
            tracked_wallet_ids: dict[str, int] = {}
            users: dict[str, dict[str, int | str]] = {}

            for idx in range(int(args.account_count)):
                label = f"user-{idx}"
                phone = benchmark_phone(str(args.phone_prefix), idx)
                user_id = int(args.user_start_id) + idx
                user = ensure_benchmark_user(user_id, phone=phone)
                wallet = create_wallet(user, "main", allow_existing=True)
                db.session.flush([wallet])
                current_balance = _read_balances({label: int(wallet.id)})[label]
                delta = int(args.initial_balance_cents) - int(current_balance)
                if delta < 0:
                    raise RuntimeError(
                        f"wallet {label} already exceeds requested balance: current={current_balance} target={args.initial_balance_cents}"
                    )
                if delta > 0:
                    bootstrap_wallet_balance(
                        wallet,
                        delta,
                        source_wallet=issuance_wallet,
                        meta_kind="JEPSEN_INIT",
                        entry_type="MINT",
                    )
                transfer_accounts.append(label)
                tracked_wallet_ids[label] = int(wallet.id)
                users[label] = {
                    "user_id": int(user.id),
                    "wallet_id": int(wallet.id),
                    "phone": str(user.phone or phone),
                }

            db.session.commit()
            db.session.refresh(issuance_wallet)
            db.session.refresh(fee_wallet)

            tracked_wallet_ids["system:fee-revenue"] = int(fee_wallet.id)
            tracked_wallet_ids["system:issuance"] = int(issuance_wallet.id)
            tracked_accounts = transfer_accounts + ["system:fee-revenue", "system:issuance"]
            balances = _read_balances(tracked_wallet_ids)
            total_amount_cents = sum(int(value) for value in balances.values())

        state = {
            "append_only": bool(args.append_only),
            "db_url": normalized_db_url,
            "max_overflow": int(args.max_overflow),
            "pool_size": int(args.pool_size),
            "sendapay_root": str(sendapay_root),
            "tracked_accounts": tracked_accounts,
            "tracked_wallet_ids": tracked_wallet_ids,
            "transfer_accounts": transfer_accounts,
            "users": users,
        }
        _save_state(args.state_file, state)
        _write_json(
            {
                "append_only": bool(args.append_only),
                "db_url": normalized_db_url,
                "total_amount_cents": int(total_amount_cents),
                "tracked_accounts": tracked_accounts,
                "transfer_accounts": transfer_accounts,
            }
        )
        return 0
    finally:
        _dispose_app(app)


def command_read(args: argparse.Namespace) -> int:
    try:
        state = _load_state(args.state_file)
        _sendapay_root, app = _build_app_from_state(state)
        try:
            with app.app_context():
                snapshot = _read_balances_consistent(state["tracked_wallet_ids"])
            _write_json({"result": "ok", **snapshot})
        finally:
            _dispose_app(app)
        return 0
    except Exception as exc:
        _write_json({"result": "info", "error": f"{exc.__class__.__name__}: {exc}"})
        return 0


def command_transfer(args: argparse.Namespace) -> int:
    try:
        state = _load_state(args.state_file)
        _sendapay_root, app = _build_app_from_state(state)
        try:
            with app.app_context():
                from app.extensions import db
                from app.models import User
                from app.services.errors import ServiceError
                from app.services.transfers import confirm_transfer_intent, quote_transfer_intent

                sender_meta = state["users"].get(str(args.from_account))
                recipient_meta = state["users"].get(str(args.to_account))
                if sender_meta is None:
                    raise RuntimeError(f"unknown sender account: {args.from_account}")
                if recipient_meta is None:
                    raise RuntimeError(f"unknown recipient account: {args.to_account}")

                sender = db.session.get(User, int(sender_meta["user_id"]))
                if sender is None:
                    raise RuntimeError(f"missing sender user_id={sender_meta['user_id']}")

                try:
                    intent = quote_transfer_intent(
                        sender,
                        {
                            "amount_cents": int(args.amount_cents),
                            "wallet_id": "main",
                            "recipient": {
                                "name": str(args.to_account),
                                "phone": str(recipient_meta["phone"]),
                            },
                            "note": f"jepsen {args.from_account}->{args.to_account}",
                        },
                    )
                except ServiceError as exc:
                    _write_json(
                        {
                            "result": "fail",
                            "status": int(exc.status_code or 400),
                            "error": str(exc.message or "quote failed"),
                        }
                    )
                    return 0

                intent_id = int(intent.id)
                payload, status = confirm_transfer_intent(
                    sender,
                    intent_id=int(intent_id),
                    include_sensitive=False,
                    request_query={},
                    request_host=None,
                    idempotency_key=f"sendapay-jepsen:{uuid.uuid4().hex}",
                    serialize_transfer_intent=_serialize_intent,
                    serialize_transaction=_serialize_transaction,
                    serialize_receipt=_serialize_receipt,
                )

            result_kind = "ok" if int(status) == 200 else ("fail" if int(status) < 500 else "info")
            _write_json(
                {
                    "result": result_kind,
                    "status": int(status),
                    "intent_id": int(intent_id),
                    "payload": payload,
                }
            )
        finally:
            _dispose_app(app)
        return 0
    except Exception as exc:
        _write_json({"result": "info", "error": f"{exc.__class__.__name__}: {exc}"})
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sendapay helper for the local Jepsen bank harness.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Provision the local Sendapay benchmark DB and actor state.")
    init.add_argument("--sendapay-root", required=True)
    init.add_argument("--state-file", required=True)
    init.add_argument("--db-url", required=True)
    init.add_argument("--account-count", type=int, required=True)
    init.add_argument("--initial-balance-cents", type=int, required=True)
    init.add_argument("--user-start-id", type=int, required=True)
    init.add_argument("--phone-prefix", default="744")
    init.add_argument("--pool-size", type=int, default=12)
    init.add_argument("--max-overflow", type=int, default=24)
    init.add_argument("--append-only", action="store_true")
    init.set_defaults(func=command_init)

    read = subparsers.add_parser("read", help="Read tracked Sendapay balances.")
    read.add_argument("--state-file", required=True)
    read.set_defaults(func=command_read)

    transfer = subparsers.add_parser("transfer", help="Execute one quoted+confirmed Sendapay transfer.")
    transfer.add_argument("--state-file", required=True)
    transfer.add_argument("--from-account", required=True)
    transfer.add_argument("--to-account", required=True)
    transfer.add_argument("--amount-cents", type=int, required=True)
    transfer.set_defaults(func=command_transfer)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
