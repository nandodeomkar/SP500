import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, date
import os

def fetch_sp500_ohlcv(start_date="1950-01-01", end_date=None):
    """
    Fetch S&P 500 OHLCV data from start_date to end_date
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    
    print(f"Fetching S&P 500 OHLCV data from {start_date} to {end_date}...")
    print("This may take a moment for such a large date range...")
    
    try:
        # Download S&P 500 data using ^GSPC ticker
        sp500_data = yf.download("^GSPC", start=start_date, end=end_date, progress=True)
        
        if sp500_data.empty:
            print("❌ No data retrieved.")
            return None
        
        # Handle MultiIndex columns if they exist
        if isinstance(sp500_data.columns, pd.MultiIndex):
            sp500_data.columns = sp500_data.columns.get_level_values(0)
        
        # Ensure we have the core OHLCV columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        available_columns = [col for col in required_columns if col in sp500_data.columns]
        
        if not available_columns:
            print("❌ No OHLCV data found in the response.")
            return None
        
        # Select only OHLCV columns (drop Adj Close and other columns)
        ohlcv_data = sp500_data[available_columns].copy()
        
        # Remove any rows with all NaN values
        ohlcv_data = ohlcv_data.dropna(how='all')
        
        print(f"✅ Successfully fetched {len(ohlcv_data):,} trading days")
        print(f"📅 Date range: {ohlcv_data.index[0].date()} to {ohlcv_data.index[-1].date()}")
        print(f"📊 Columns: {list(ohlcv_data.columns)}")
        
        return ohlcv_data
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

def save_data(data, filename="sp500_ohlcv_1950_to_present"):
    """
    Save the OHLCV data to CSV and Excel files
    """
    if data is None:
        print("❌ No data to save.")
        return
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    try:
        # Save as CSV
        csv_path = f"data/{filename}.csv"
        data.to_csv(csv_path)
        print(f"✅ Saved CSV: {csv_path}")
        
        # Save as Excel
        excel_path = f"data/{filename}.xlsx"
        data.to_excel(excel_path)
        print(f"✅ Saved Excel: {excel_path}")
        
        # Show file sizes
        csv_size = os.path.getsize(csv_path) / (1024*1024)
        excel_size = os.path.getsize(excel_path) / (1024*1024)
        print(f"📁 File sizes: CSV={csv_size:.1f}MB, Excel={excel_size:.1f}MB")
        
        return csv_path, excel_path
        
    except Exception as e:
        print(f"❌ Error saving files: {e}")
        return None, None

def analyze_ohlcv_data(data):
    """
    Print summary statistics for the OHLCV data
    """
    if data is None:
        return
    
    print("\n" + "="*70)
    print("S&P 500 OHLCV DATA ANALYSIS (1950 - PRESENT)")
    print("="*70)
    
    # Basic info
    print(f"📈 Total trading days: {len(data):,}")
    print(f"📅 Date range: {data.index[0].date()} to {data.index[-1].date()}")
    print(f"📆 Years covered: {(data.index[-1] - data.index[0]).days / 365.25:.1f}")
    
    # Price analysis
    if 'Close' in data.columns:
        print(f"\n💰 PRICE STATISTICS:")
        print(f"   First close price (1950): ${data['Close'].iloc[0]:.2f}")
        print(f"   Latest close price: ${data['Close'].iloc[-1]:.2f}")
        print(f"   All-time low: ${data['Close'].min():.2f} on {data['Close'].idxmin().date()}")
        print(f"   All-time high: ${data['Close'].max():.2f} on {data['Close'].idxmax().date()}")
        
        # Calculate returns
        total_return = ((data['Close'].iloc[-1] / data['Close'].iloc[0]) - 1) * 100
        years = (data.index[-1] - data.index[0]).days / 365.25
        annual_return = ((data['Close'].iloc[-1] / data['Close'].iloc[0]) ** (1/years) - 1) * 100
        
        print(f"\n📊 RETURNS:")
        print(f"   Total return since 1950: {total_return:,.1f}%")
        print(f"   Annualized return: {annual_return:.1f}%")
    
    # Volume analysis
    if 'Volume' in data.columns:
        avg_volume = data['Volume'].mean()
        max_volume = data['Volume'].max()
        max_volume_date = data['Volume'].idxmax().date()
        
        print(f"\n📈 VOLUME STATISTICS:")
        print(f"   Average daily volume: {avg_volume:,.0f}")
        print(f"   Highest volume day: {max_volume:,.0f} on {max_volume_date}")
    
    # Data quality check
    print(f"\n🔍 DATA QUALITY:")
    for col in data.columns:
        missing = data[col].isnull().sum()
        print(f"   {col}: {missing:,} missing values ({missing/len(data)*100:.2f}%)")

def main():
    """
    Main function to execute the data fetching process
    """
    print("🚀 Starting S&P 500 OHLCV Data Collection...")
    print("📊 Fetching daily data from 1950 to present day")
    
    # Fetch the data
    sp500_ohlcv = fetch_sp500_ohlcv(start_date="1950-01-01")
    
    if sp500_ohlcv is not None:
        # Analyze the data
        analyze_ohlcv_data(sp500_ohlcv)
        
        # Save the data
        print(f"\n💾 Saving data...")
        csv_file, excel_file = save_data(sp500_ohlcv)
        
        # Show sample data
        print(f"\n📋 SAMPLE DATA:")
        print("First 5 rows:")
        print(sp500_ohlcv.head())
        print("\nLast 5 rows:")
        print(sp500_ohlcv.tail())
        
        # Final summary
        print(f"\n🎉 SUCCESS!")
        print(f"✅ Downloaded {len(sp500_ohlcv):,} trading days of S&P 500 OHLCV data")
        print(f"📁 Files saved in 'data' folder")
        print(f"📈 Data spans {(sp500_ohlcv.index[-1] - sp500_ohlcv.index[0]).days / 365.25:.1f} years")
        
        return sp500_ohlcv
    else:
        print("❌ Failed to fetch S&P 500 data")
        return None

# Execute the script
if __name__ == "__main__":
    data = main()