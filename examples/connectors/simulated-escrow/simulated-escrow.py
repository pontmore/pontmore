#!/usr/bin/python3
"""Long-lived, no-value escrow connector for exercising the connector contract."""

import json
import os
import sys


def respond(request: dict, status: str, result: dict, error: str | None = None) -> None:
    print(
        json.dumps(
            {
                "version": 1,
                "request_id": request["request_id"],
                "status": status,
                "result": result,
                "error": error,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def main() -> None:
    outcomes: dict[str, str] = {}
    idempotent_outcomes: dict[str, str] = {}
    for line in sys.stdin:
        request = json.loads(line)
        command = request["command"]
        command_type = command["type"]
        if request["version"] != 1:
            respond(request, "failed", {}, "unsupported connector API version")
        elif command_type in {"hello", "health"}:
            respond(request, "succeeded", {"pid": os.getpid(), "healthy": True})
        elif command_type == "execute":
            operation_id = command["operation_id"]
            idempotency_key = command["idempotency_key"]
            failure = command.get("payload", {}).get("inject_failure")
            if failure == "exit":
                return
            elif failure == "ambiguous":
                respond(request, "ambiguous", {}, "simulated outcome is unknown")
            elif failure == "failed":
                outcomes[operation_id] = "failed"
                idempotent_outcomes[idempotency_key] = "failed"
                respond(request, "failed", {}, "simulated operation failed")
            else:
                outcome = idempotent_outcomes.setdefault(idempotency_key, "succeeded")
                outcomes[operation_id] = outcome
                respond(
                    request,
                    outcome,
                    {
                        "pid": os.getpid(),
                        "operation_id": operation_id,
                        "idempotency_key": idempotency_key,
                        "operation": command["operation"],
                        "simulated": True,
                    },
                )
        elif command_type == "inspect":
            operation_id = command["operation_id"]
            outcome = outcomes.get(operation_id, "ambiguous")
            respond(
                request,
                outcome,
                {
                    "pid": os.getpid(),
                    "operation_id": operation_id,
                    "status": outcome,
                },
                None if outcome == "succeeded" else "simulated outcome is not successful",
            )
        elif command_type == "shutdown":
            respond(request, "succeeded", {"stopping": True})
            return
        else:
            respond(request, "failed", {}, "unsupported command")


if __name__ == "__main__":
    main()
