"""Database layer for CFRD Support Bot"""

from .connection import get_connection, setup_database
from .queries import (
    save_client,
    get_client_by_thread,
    update_client_thread,
    save_message,
    save_manager,
    get_manager,
    update_manager_photo,
    get_all_active_threads,
    save_first_reply,
    is_first_reply,
    get_managers_replied_to_client,
    get_client_info_by_thread,
    update_client_message_time,
    update_manager_reply_time,
    assign_duty_manager,
    get_duty_manager,
    unpack_manager_data,
)

__all__ = [
    "get_connection",
    "setup_database",
    "save_client",
    "get_client_by_thread",
    "update_client_thread",
    "save_message",
    "save_manager",
    "get_manager",
    "update_manager_photo",
    "get_all_active_threads",
    "save_first_reply",
    "is_first_reply",
    "get_managers_replied_to_client",
    "get_client_info_by_thread",
    "update_client_message_time",
    "update_manager_reply_time",
    "assign_duty_manager",
    "get_duty_manager",
    "unpack_manager_data",
]
