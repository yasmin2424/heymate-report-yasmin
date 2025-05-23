from typing import List, Dict, Any, Union, Iterable
import pandas as pd
import random
import json
import time
from datetime import datetime, timezone
import requests
from openai import OpenAI

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

MODEL_NAME = "o4-mini"

def get_default_allowed_types() -> List[str]:
    """
    Returns the default list of standardized restaurant types.

    These types are based on the "Food and Drink" place types defined by Google Maps Places:
    https://developers.google.com/maps/documentation/places/web-service/place-types#food-and-drink

    If future updates or customizations to the allowed restaurant types are needed,
    this list should be modified accordingly.

    Returns:
        List[str]: A list of valid restaurant type strings for standardization.
    """
    return [
        "acai shop", "afghani restaurant", "african restaurant", "american restaurant", "asian restaurant",
        "bagel shop", "bakery", "bar", "bar and grill", "barbecue restaurant", "brazilian restaurant",
        "breakfast restaurant", "brunch restaurant", "buffet restaurant", "cafe", "cafeteria", "candy store",
        "cat cafe", "chinese restaurant", "chocolate factory", "chocolate shop", "coffee shop", "confectionery",
        "deli", "dessert restaurant", "dessert shop", "diner", "dog cafe", "donut shop", "fast food restaurant",
        "fine dining restaurant", "food court", "french restaurant", "greek restaurant", "hamburger restaurant",
        "ice cream shop", "indian restaurant", "indonesian restaurant", "italian restaurant", "japanese restaurant",
        "juice shop", "korean restaurant", "lebanese restaurant", "meal delivery", "meal takeaway", "mediterranean restaurant",
        "mexican restaurant", "middle eastern restaurant", "pizza restaurant", "pub", "ramen restaurant", "restaurant",
        "sandwich shop", "seafood restaurant", "spanish restaurant", "steak house", "sushi restaurant", "tea house",
        "thai restaurant", "turkish restaurant", "vegan restaurant", "vegetarian restaurant", "vietnamese restaurant", "wine bar"
    ]

# ─────────────────────────────────────────────────────────────
# OpenAI API Connector
# ─────────────────────────────────────────────────────────────

class OpenAIConnector:
    """
    Wrapper class to handle interaction with the OpenAI Chat API.

    This class is responsible for:
    - Loading the OpenAI API token from a file.
    - Constructing the prompt.
    - Sending batch requests to the GPT model.
    - Parsing and returning structured results.

    Attributes:
    ----------
    token_path : str
        Path to the file that stores the OpenAI API key.

    Methods:
    -------
    classify_batch(rows, model=..., timeout_s=...)
        Send a batch of menu items to the GPT model for structured extraction.

    _load_token(token_path)
        Load the API token from a local credentials file.

    _get_system_prompt()
        Generate the system instruction used in GPT prompt.
    """
    def __init__(self, token_path="../credentials/open_ai_token.txt"):
        self.token = self._load_token(token_path)
        self.client = OpenAI(api_key=self.token)

    @staticmethod
    def _load_token(token_path):
        try:
            with open(token_path, "r") as f:
                token = f.read().strip()
                if not token:
                    raise ValueError("Token file is empty.")
                return token
        except FileNotFoundError:
            raise FileNotFoundError(f"Token file not found at {token_path}")
        except Exception as e:
            raise RuntimeError(f"Error reading token: {e}")

    def classify_batch(
        self,
        rows: Union[str, Iterable[Dict[str, str]]],
        model: str = MODEL_NAME,
        timeout_s: int = 60
    ) -> List[Dict[str, Any]]:
        system_prompt = self._get_system_prompt()
        if isinstance(rows, str):
            rows = json.loads(rows)
        user_msg = json.dumps(rows, indent=2)

        resp = self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            timeout=timeout_s
        )

        content = resp.choices[0].message.content
        try:
            results = json.loads(content)
        except json.JSONDecodeError:
            raise ValueError("❌ Failed to parse response as JSON:\n" + content)
        return results

    def _get_system_prompt(self) -> str:
        restaurant_type_list = ", ".join(f'"{r}"' for r in get_default_allowed_types())
        return f"""
You are a Menu Data Extractor.

Input: A list of menu items. Each item includes:
  - restaurant_name
  - restaurant_type
  - item_name
  - menu_item_description (may be empty)
  - menu_category (may be empty)

Output: a JSON array (same order) with:
  - dish_base : string (the primary dish name, e.g., "pizza")
  - dish_flavor : string[] (up to 5 flavour descriptors, e.g., "pepperoni")
  - is_combo : boolean
  - restaurant_type_std : string

Rules:
  • Use lowercase American English, and translate non-English terms into English based on Merriam-Webster and AP Stylebook.
  
  • dish_base:
      - Should be the main identity of the dish (e.g., "pizza", "lo mein", "fried rice").
      - Remove size indicators, quantity counts, and side items.
      - If the base is unclear or ambiguous, use "unknown".
      - Use singular form.
      - Use menu_name and context from other inputs to identify dish_base.

  • dish_flavor:
      - Up to 5 descriptors of flavor, cooking style, toppings, sauces, etc.
      - Each tag must be no more than two words.
      - DO NOT repeat dish_base unless meaning is added.
      - Use singular form and lowercase.

  • is_combo:
      - True if the item clearly bundles multiple components.

  • restaurant_type_std:
      - Must match one of the following values exactly: {restaurant_type_list}
      - If `restaurant_type` partially matches or contains keywords, normalize accordingly.
      - Otherwise, infer from all input fields.

  • Output must be raw JSON. No Markdown, no backticks, no labels.
"""

# ─────────────────────────────────────────────────────────────
# Main Batch Processing Function
# ─────────────────────────────────────────────────────────────

def run_qc_extraction(
    df_input: pd.DataFrame,
    col_mapping: Dict[str, str] = None,
    allowed_types: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Process a DataFrame of menu items and extract standardized dish features using an OpenAI LLM.

    Parameters:
    ----------
    df_input : pd.DataFrame
        The input DataFrame containing raw menu item data. Must include the following columns:
        - 'row_id', 'item_id', 'restaurant_name', 'restaurant_type', 'item_name', 'menu_item_description', 'menu_category'

    col_mapping : dict, optional
        Optional dictionary to map custom input column names to the expected column names.

    allowed_types : list of str, optional
        List of valid standardized restaurant types. If not provided, defaults to a predefined list.

    Returns:
    -------
    List[Dict[str, Any]]
        A list of dictionaries where each dictionary includes:
        - 'row_id': Original row ID from the input
        - 'item_id': Original item ID from the input
        - 'dish_base': Extracted base name of the dish (e.g., "pizza")
        - 'dish_flavor': List of up to 5 descriptors (e.g., toppings, sauces)
        - 'is_combo': Boolean indicating if the item is a combo
        - 'restaurant_type_std': Standardized restaurant type string

    Raises:
    ------
    ValueError:
        If any of the extracted restaurant_type_std values are not in the allowed types list.
    """
    if col_mapping is None:
        col_mapping = {}
    if allowed_types is None:
        allowed_types = get_default_allowed_types()

    df = df_input.rename(columns=col_mapping).copy()
    expected_cols = ["row_id", "item_id", "restaurant_name", "restaurant_type", "item_name", "menu_item_description", "menu_category"]  
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    df_batch = df[expected_cols]

    # Call GPT
    oai = OpenAIConnector()
    result = oai.classify_batch(df_batch.to_dict(orient='records'))

    # Build JSON output
    output_json = []
    for row_id, item_id, res in zip(df_batch["row_id"].values, df_batch["item_id"].values, result): 
        res = res or {}
        record = {
            "row_id": row_id,  
            "item_id": item_id,
            "dish_base": res.get("dish_base", ""),
            "dish_flavor": res.get("dish_flavor", []),
            "is_combo": res.get("is_combo", False),
            "restaurant_type_std": res.get("restaurant_type_std", "")
        }
        output_json.append(record)

    # Validate restaurant type
    found_types = set(str(r.get("restaurant_type_std", "")).strip().lower() for r in output_json)
    allowed_set = set(v.strip().lower() for v in allowed_types)
    invalid_values = found_types - allowed_set
    if invalid_values:
        raise ValueError(f"Invalid restaurant_type_std values found: {sorted(invalid_values)}")

    return output_json

# ─────────────────────────────────────────────────────────────
# Main Execution Entry Point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas as pd

    # Example use case: direct DataFrame input
    df_example = pd.DataFrame([
        {
            "row_id": "001",
            "item_id": "1",
            "restaurant_name": "Pizza World",
            "restaurant_type": "Fast Food, Pizza",
            "item_name": "Pepperoni Pizza Combo",
            "menu_item_description": "Large pepperoni pizza with garlic bread and a drink",
            "menu_category": "Combo"
        }
    ])

    try:
        results = run_qc_extraction(df_input=df_example)
        print("✅ Extraction completed. Output:")
        print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
