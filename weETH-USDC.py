import requests
import json
from datetime import datetime, timedelta
import csv
import math

def normalize_value(value, decimals):
    try:
        return float(value) / (10 ** decimals)
    except (TypeError, ValueError):
        print(f"Error normalizing value: {value}")
        return None

def get_block_from_timestamp(timestamp, block_subgraph_url):
    query_template = f"""
    {{
      blocks(first: 1, orderBy: timestamp, orderDirection: asc, where: {{timestamp_gte: {timestamp}}}) {{
        number
        timestamp
      }}
    }}
    """
    headers = {'Content-Type': 'application/json'}
    response = requests.post(block_subgraph_url, json={'query': query_template}, headers=headers)

    if response.status_code != 200:
        print(f"Error querying block subgraph: {response.status_code}, {response.text}")
        return None

    data = response.json()

    if "errors" in data:
        print(f"GraphQL errors: {data['errors']}")
        return None

    blocks = data.get("data", {}).get("blocks", [])
    if not blocks:
        print(f"No blocks found for timestamp: {timestamp}")
        return None

    block = blocks[0]
    print(f"Found block {block['number']} for timestamp {timestamp}")
    return block["number"]

def query_pool_at_block(graph_url, pool_id, block):
    query_template = f"""
    {{
      gammaPool(id: "{pool_id}", block: {{number: {block}}}) {{
        id
        lpInvariant
        lpBorrowedInvariant
        lastPrice
        totalSupply
        token0 {{ decimals }}
        token1 {{ decimals }}
      }}
    }}
    """
    headers = {'Content-Type': 'application/json'}
    response = requests.post(graph_url, json={'query': query_template}, headers=headers)
    
    if response.status_code != 200:
        print(f"Error querying subgraph: {response.status_code}, {response.text}")
        return None

    data = response.json()

    if "errors" in data:
        print(f"GraphQL errors: {data['errors']}")
        return None

    return data.get("data", {}).get("gammaPool", None)

def save_to_csv(data, filename="weETH-USDC_Data.csv"):
    file_exists = False
    try:
        with open(filename, 'r') as file:
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def main():
    arb_subgraph_url = "https://api.goldsky.com/api/public/project_clut9lukx80ry01xb5ngf1zmj/subgraphs/gammaswap-v1-arbitrum/prod/gn"
    #add your subgraph api key here
    block_subgraph_url = "https://gateway.thegraph.com/api/[your_api_key]/subgraphs/id/JBnWrv9pvBvSi2pUZzba3VweGBTde6s44QvsDABP47Gt"
    pool_id = "0xd63c125b169bc5655f9fdefb47c7d33e622416c7"
    
    start_timestamp = int(datetime(2024, 3, 29, 5, 32, 46).timestamp())
    current_timestamp = start_timestamp
    today_timestamp = int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    pool_creation_data = None

    while current_timestamp <= today_timestamp:
        block_number = get_block_from_timestamp(current_timestamp, block_subgraph_url)
        if not block_number:
            print(f"No block found for timestamp: {current_timestamp}")
            current_timestamp += 7 * 86400  # Increment by 1 week
            continue

        pool_data = query_pool_at_block(arb_subgraph_url, pool_id, block_number)
        if pool_data:
            print(f"Data for block {block_number} retrieved.")
            
            #note some of these values are hardcoded. Last_price should be in token1 decimals
            #and invariants should be the average of token0 & token1 decimals
            token0_decimals = int(pool_data["token0"]["decimals"])
            token1_decimals = int(pool_data["token1"]["decimals"])
            lp_invariant = normalize_value(pool_data["lpInvariant"], 12)
            lp_borrowed_invariant = normalize_value(pool_data["lpBorrowedInvariant"], 12)
            last_price = normalize_value(pool_data["lastPrice"], 6)
            total_supply = normalize_value(pool_data["totalSupply"], 12)

            if pool_creation_data is None:
                pool_creation_data = {
                    "totalInvariant": lp_invariant + lp_borrowed_invariant,
                    "totalSupply": total_supply,
                    "lastPrice": last_price,
                }
            print(f"Normalized values at block {block_number}:")
            print(f"  lpInvariant: {lp_invariant}")
            print(f"  lpBorrowedInvariant: {lp_borrowed_invariant}")
            print(f"  lastPrice: {last_price}")
            print(f"  totalSupply: {total_supply}")

            total_invariant = lp_invariant + lp_borrowed_invariant

            total_lp_return_percent = None
            if pool_creation_data:
                total_lp_return_percent = (
                    ((total_invariant / pool_creation_data["totalInvariant"]) /
                     (total_supply / pool_creation_data["totalSupply"])) *
                    math.sqrt(last_price / pool_creation_data["lastPrice"]) 
                ) - 1

            spot_return_percent = None
            if pool_creation_data:
                spot_return_percent = (((last_price / pool_creation_data["lastPrice"]) * 0.5 + 0.5) / 1) - 1

            utc_timestamp = datetime.utcfromtimestamp(current_timestamp).strftime('%Y-%m-%d %H:%M:%S')

            csv_row = {
                "utcTimestamp": utc_timestamp,
                "blockNumber": block_number,
                "totalInvariant": round(total_invariant, 6),
                "totalSupply": round(total_supply, 6),
                "lastPrice": round(last_price, 6),
                "totalLPReturnPercent": round(total_lp_return_percent, 6) if total_lp_return_percent is not None else None,
                "spotReturnPercent": round(spot_return_percent, 6) if spot_return_percent is not None else None,
            }
            save_to_csv(csv_row, "weETH-USDC_Data.csv")
            print(f"Row written: {csv_row}")

            # Increment timestamp weekly
            current_timestamp += 7 * 86400

        else:
            print(f"No data returned for block {block_number}")
            current_timestamp += 7 * 86400

if __name__ == "__main__":
    main()