import json
from datetime import datetime, timedelta


class DateRangeUpdater:
    """
    Automatically updates date range fields in an API payload JSON.

    Logic requested:
        - If created_from & created_to exist → update them.
        - If fulfilled_from & fulfilled_to exist → update them.
        - If both exist → update both.
        - If a pair does not exist → leave it untouched.

    Timestamp format applied:
        YYYY-MM-DDTHH:MM:SS.mmmZ
    """

    @staticmethod
    def get_yesterday_range():
        startday = datetime.utcnow() - timedelta(days=7)
        endday = datetime.utcnow() - timedelta(days=1)

        start = startday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = endday.replace(hour=23, minute=59, second=59, microsecond=999000)

        return (
            start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        )

    @classmethod
    def update_payload_file(cls, file_path: str):
        """Load JSON, update date ranges, save back."""
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        start, end = cls.get_yesterday_range()

        filters = data.get("filters", {})

        # ---- CREATED RANGE ----
        has_created = "created_from" in filters and "created_to" in filters
        if has_created:
            filters["created_from"] = start
            filters["created_to"] = end

        # ---- FULFILLED RANGE ----
        has_fulfilled = "fulfilled_from" in filters and "fulfilled_to" in filters
        if has_fulfilled:
            filters["fulfilled_from"] = start
            filters["fulfilled_to"] = end
        
        has_completed = "completed_from" in filters and "completed_to" in filters
        if has_completed:
            filters["completed_from"] = start
            filters["completed_to"] = end

        # Save updated filters back
        data["filters"] = filters

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        return file_path
