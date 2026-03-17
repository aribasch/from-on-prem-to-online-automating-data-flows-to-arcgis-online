from datetime import datetime, timezone, timedelta

class TableLogger:
    def __init__(self, gis, table_id):
        """
        gis: ArcGIS Online GIS object
        table_id: Item ID of the AGOL logging table
        """
        item = gis.content.get(table_id)
        # Use the first table in the item
        self.agol_table = item.tables[0] if hasattr(item, 'tables') and item.tables else item

    def log(self, message):
        """
        Logs a message to the AGOL logging table.
        """
        log_entry = {
            'attributes': {
                'datetimestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'message': message
            }
        }
        result = self.agol_table.edit_features(adds=[log_entry])
        return result

    def clean_old_logs(self):
        """
        Deletes log records older than 6 months from the AGOL logging table.
        """
        six_months_ago = datetime.now(timezone.utc) - timedelta(days=182)
        cutoff_str = six_months_ago.strftime('%Y-%m-%d %H:%M:%S')
        # Query for logs older than cutoff
        query = f"datetimestamp < '{cutoff_str}'"
        old_logs = self.agol_table.query(where=query, return_ids_only=True)
        if old_logs and 'objectIds' in old_logs and old_logs['objectIds']:
            self.agol_table.edit_features(deletes=old_logs['objectIds'])