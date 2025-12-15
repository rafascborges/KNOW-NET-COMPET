from elt_core.base_source import BaseDataSource

class MarketingSource(BaseDataSource):
    source_name = "marketing"
    def transform(self, data):
        """
        Normalize input: lowercase email addresses, rename keys.
        """
        cleaned_data = []
        
        # Handle if data is a single dict or list
        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            raise ValueError("Data must be a dict or list of dicts")

        for item in items:
            new_item = {}
            
            # Normalize email
            email = item.get('email') or item.get('Email')
            if email:
                new_item['email'] = email.lower()
            
            # Rename 'full_name' or 'Name' to 'name'
            name = item.get('full_name') or item.get('Name') or item.get('name')
            if name:
                new_item['name'] = name
            
            # Preserve ID if present
            if 'id' in item:
                new_item['id'] = item['id']
                
            # Add any other fields you want to preserve
            # For example, 'campaign'
            if 'campaign' in item:
                new_item['campaign'] = item['campaign']

            cleaned_data.append(new_item)
            
        return cleaned_data
