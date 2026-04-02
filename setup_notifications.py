"""
setup_notifications.py
Configure Prefect notification blocks for pipeline alerts.

Supports:
- Slack webhooks
- Email (SMTP)
- Discord webhooks
- Microsoft Teams webhooks

Usage:
    python setup_notifications.py slack    # Setup Slack webhook
    python setup_notifications.py email    # Setup email notifications
    python setup_notifications.py list     # List existing blocks
    python setup_notifications.py delete   # Delete all notification blocks
"""

import argparse
import os


def setup_slack_webhook(webhook_url: str = None):
    """Create a Slack webhook notification block."""
    from prefect.blocks.notifications import SlackWebhook
    
    # Get webhook URL from arg or environment
    webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("Error: Slack webhook URL required.")
        print("Pass as argument or set SLACK_WEBHOOK_URL environment variable.")
        print("\nTo create a Slack webhook:")
        print("1. Go to https://api.slack.com/apps")
        print("2. Create new app → From scratch")
        print("3. Enable Incoming Webhooks")
        print("4. Add new webhook to channel")
        return False
    
    try:
        slack_block = SlackWebhook(
            url=webhook_url,
        )
        slack_block.save("pipeline-alerts")
        print("✓ Slack webhook block 'pipeline-alerts' created successfully")
        return True
    except Exception as e:
        print(f"Error creating Slack block: {e}")
        return False


def setup_email_notification():
    """Create email notification block with SMTP settings."""
    from prefect.blocks.notifications import EmailServer
    
    # Email configuration from environment
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    
    # Recipients
    recipients = os.getenv("ALERT_RECIPIENTS", "").split(",")
    
    if not all([smtp_user, smtp_password]):
        print("Error: SMTP credentials required.")
        print("Set the following environment variables:")
        print("  SMTP_USER=your-email@gmail.com")
        print("  SMTP_PASSWORD=your-app-password")
        print("  SMTP_HOST=smtp.gmail.com")
        print("  SMTP_PORT=587")
        print("  ALERT_RECIPIENTS=team@company.com,oncall@company.com")
        return False
    
    try:
        email_block = EmailServer(
            host=smtp_host,
            port=smtp_port,
            username=smtp_user,
            password=smtp_password,
            from_email=smtp_user,
            to_emails=recipients,
        )
        email_block.save("pipeline-email-alerts")
        print("✓ Email notification block 'pipeline-email-alerts' created successfully")
        return True
    except Exception as e:
        print(f"Error creating email block: {e}")
        return False


def setup_discord_webhook(webhook_url: str = None):
    """Create a Discord webhook notification block."""
    from prefect.blocks.notifications import SlackWebhook  # Discord uses same format
    
    webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
    
    if not webhook_url:
        print("Error: Discord webhook URL required.")
        print("Pass as argument or set DISCORD_WEBHOOK_URL environment variable.")
        print("\nTo create a Discord webhook:")
        print("1. Go to your Discord server settings")
        print("2. Integrations → Webhooks")
        print("3. Create webhook")
        return False
    
    try:
        discord_block = SlackWebhook(
            url=webhook_url,
        )
        discord_block.save("pipeline-discord-alerts")
        print("✓ Discord webhook block 'pipeline-discord-alerts' created successfully")
        return True
    except Exception as e:
        print(f"Error creating Discord block: {e}")
        return False


def create_notification_rule(name: str, block_name: str, tags: list = None):
    """Create a notification rule that triggers on flow states."""
    from prefect.blocks.system import NotificationBlock
    
    tags = tags or ["etl", "morocco-re"]
    
    notification = NotificationBlock(
        block_name=f"notify-{name}",
        notify_on=[
            "flow.failed",
            "flow.crashed",
            "flow.timed_out",
        ],
        tags=tags,
    )
    notification.save()
    print(f"✓ Notification rule 'notify-{name}' created successfully")


def list_blocks():
    """List all existing notification blocks."""
    from prefect.blocks.core import Block
    
    print("\n=== Notification Blocks ===")
    
    # List all SlackWebhook blocks
    print("\nSlack Webhooks:")
    for block in Block.list(block_type_slug="slack-webhook"):
        print(f"  - {block.name}")
    
    # List all EmailServer blocks
    print("\nEmail Servers:")
    for block in Block.list(block_type_slug="email-server"):
        print(f"  - {block.name}")
    
    # List all Notification blocks
    print("\nNotification Rules:")
    for block in Block.list(block_type_slug="notification"):
        print(f"  - {block.name}")
    
    print()


def delete_block(block_type: str, block_name: str):
    """Delete a notification block."""
    from prefect.blocks.core import Block
    
    try:
        Block.delete(block_type=block_type, name=block_name)
        print(f"✓ Block '{block_name}' deleted successfully")
    except Exception as e:
        print(f"Error deleting block: {e}")


def delete_all_notification_blocks():
    """Delete all notification-related blocks."""
    from prefect.blocks.core import Block
    
    blocks_to_delete = []
    
    for block in Block.list(block_type_slug="slack-webhook"):
        blocks_to_delete.append(("slack-webhook", block.name))
    
    for block in Block.list(block_type_slug="email-server"):
        blocks_to_delete.append(("email-server", block.name))
    
    for block in Block.list(block_type_slug="notification"):
        blocks_to_delete.append(("notification", block.name))
    
    if not blocks_to_delete:
        print("No notification blocks to delete")
        return
    
    print(f"Found {len(blocks_to_delete)} blocks to delete:")
    for block_type, block_name in blocks_to_delete:
        print(f"  - {block_name} ({block_type})")
    
    confirm = input("\nDelete all? (y/N): ")
    if confirm.lower() == "y":
        for block_type, block_name in blocks_to_delete:
            delete_block(block_type, block_name)
    else:
        print("Cancelled")


def setup_all_notifications():
    """Setup all notification channels."""
    print("Setting up all notifications...\n")
    
    print("1. Setting up Slack...")
    setup_slack_webhook()
    
    print("\n2. Setting up Email...")
    setup_email_notification()
    
    print("\n3. Creating notification rules...")
    create_notification_rule("slack", "pipeline-alerts")
    create_notification_rule("email", "pipeline-email-alerts")
    
    print("\n" + "=" * 50)
    print("Notification setup complete!")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Run 'prefect backend server' to start Prefect server")
    print("2. Run 'prefect server start' to open Prefect UI")
    print("3. Deploy your pipeline: python deployment.py create")


# ---------------------------------------------------------------------------
# EXAMPLE: INTEGRATE WITH FLOW
# ---------------------------------------------------------------------------

INTEGRATION_EXAMPLE = '''
# Add to prefect_flow.py to use notifications:

from prefect.blocks.notifications import SlackWebhook

@flow(
    name="morocco-re-pipeline",
    on_failure=[SlackWebhook.load("pipeline-alerts")],
)
def run_pipeline_flow(...):
    ...

# Or trigger manually:
slack = SlackWebhook.load("pipeline-alerts")
slack.notify("Pipeline failed: {error_message}")
'''


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefect Notification Setup")
    parser.add_argument(
        "command",
        choices=["slack", "email", "discord", "all", "list", "delete", "help"],
        help="Notification channel to setup",
    )
    parser.add_argument("--webhook-url", help="Webhook URL for Slack/Discord")
    
    args = parser.parse_args()
    
    if args.command == "slack":
        setup_slack_webhook(args.webhook_url)
    elif args.command == "email":
        setup_email_notification()
    elif args.command == "discord":
        setup_discord_webhook(args.webhook_url)
    elif args.command == "all":
        setup_all_notifications()
    elif args.command == "list":
        list_blocks()
    elif args.command == "delete":
        delete_all_notification_blocks()
    elif args.command == "help":
        print(INTEGRATION_EXAMPLE)
