# scripts/fetch_ksamsok_data.py
import pandas as pd
import re
import json
from ksamsok import KSamsok
import time

def fetch_photo_data(batch_size=100):
    """
    Fetches photo data from KSamsok (serviceOrganization=s-om),
    filters by year (1900-2010), and returns a pandas DataFrame.
    """
    ks = KSamsok()
    query = 'itemType=foto AND thumbnailExists=j AND serviceOrganization=s-om'
    print(f"Starting fetch for query: '{query}'")

    all_data = []
    start_index = 1
    total_hits_str = 'unknown' # Will get this from the first response
    processed_records = 0

    # Regex to extract the first year from time_label like "YYYY - YYYY" or just "YYYY"
    time_label_regex = re.compile(r'^(\d{4})')

    # Loop indefinitely until no more records are returned
    while True:
        print(f"Fetching batch starting at index {start_index} (found {len(all_data)} matching photos so far)...", end='\r')
        try:
            # Fetch a batch of results
            results = ks.cql(query, start=start_index, hits=batch_size)
            time.sleep(0.5) # Be nice to the API

            if not results or 'records' not in results or not results['records']:
                print("\nNo more records found or error in response.")
                break

            if start_index == 1:
                total_hits_str = results.get('hits', 'unknown')
                print(f"Total potential hits according to API: {total_hits_str}")

            records = results['records']
            processed_records += len(records)

            for item in records:
                try:
                    pres = item.get('presentation', {})
                    year = None

                    # Extract year from contexts/time_label
                    contexts = pres.get('contexts')
                    if contexts and isinstance(contexts, list) and len(contexts) > 0:
                        time_label = contexts[0].get('time_label')
                        if time_label:
                            match = time_label_regex.match(time_label)
                            if match:
                                year_str = match.group(1)
                                try:
                                    year = int(year_str)
                                except ValueError:
                                    year = None
                    
                    # Filter by year (Updated Range: 1900-2010)
                    if year and 1900 <= year <= 2010:
                        image_url = None
                        if pres.get('images'):
                            image_url = pres['images'][0].get('lowres') or pres['images'][0].get('thumbnail')

                        if image_url:
                            all_data.append({
                                'id': pres.get('uri'),
                                'year': year,
                                'image_url': image_url,
                            })
                            # No longer breaking based on target_count

                except Exception as e:
                    print(f"\nError processing item {pres.get('uri', 'unknown')}: {e}")
                    continue # Skip problematic items

            # Prepare for next batch
            start_index += len(records)
            
            # Stop if the last batch was smaller than requested (indicates end of results)
            if len(records) < batch_size:
                print("\nLast batch was smaller than requested size, assuming end of results.")
                break

        except Exception as e:
            print(f"\nError fetching batch starting at {start_index}: {e}")
            break # Stop on fetch error

    print(f"\nFinished fetching. Processed ~{processed_records} records. Found {len(all_data)} photos between 1900-2010.")

    if not all_data:
        return None

    # Create DataFrame
    df = pd.DataFrame(all_data)
    print("\nSample data:")
    print(df.head())
    print(f"\nYear distribution:\n{df['year'].value_counts().sort_index()}")

    return df

if __name__ == "__main__":
    # Fetch all matching photos (no target count)
    photo_df = fetch_photo_data()

    if photo_df is not None and not photo_df.empty:
        # Use a fixed filename for overwriting
        output_filename = "ksamsok_s-om_photos_1900_2010.csv"
        try:
            photo_df.to_csv(output_filename, index=False)
            print(f"\nData saved to {output_filename}")
        except Exception as e:
             print(f"\nError saving DataFrame to CSV: {e}")
    else:
        print("\nNo data fetched or DataFrame is empty, not saving CSV.") 