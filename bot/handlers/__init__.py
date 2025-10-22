"""Event handlers for Telegram bot"""

# All handlers are registered in their respective modules
# Import them to register with the client

from . import client_messages
from . import manager_commands

__all__ = ["client_messages", "manager_commands"]
