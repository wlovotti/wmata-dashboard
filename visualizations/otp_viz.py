"""
On-Time Performance Visualization

Generates various visualizations of on-time performance metrics:
- Overall OTP rate (pie chart)
- OTP over time (line chart)
- Lateness distribution (histogram)
- OTP by direction (bar chart)

Usage:
    python visualizations/otp_viz.py C51
    python visualizations/otp_viz.py C53 --output-dir custom_output/
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from src.database import get_session
from src.models import VehiclePosition
from src.analytics import calculate_on_time_performance

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


def collect_otp_data(route_id: str):
    """
    Collect OTP data for a route from the database.

    Returns:
        pandas DataFrame with columns: timestamp, vehicle_id, trip_id,
        lateness_minutes, is_on_time, confidence, direction_id
    """
    db = get_session()

    try:
        # Get all vehicle positions for this route
        positions = db.query(VehiclePosition).filter(
            VehiclePosition.route_id == route_id
        ).order_by(VehiclePosition.timestamp).all()

        if not positions:
            print(f"No vehicle positions found for route {route_id}")
            return None

        print(f"Found {len(positions)} vehicle positions for route {route_id}")

        # Calculate OTP using analytics module
        print("Calculating on-time performance...")
        otp_results = calculate_on_time_performance(db, route_id)

        if not otp_results or otp_results.get('arrivals_analyzed', 0) == 0:
            print("No OTP results calculated")
            return None

        # Get sample arrivals and build DataFrame
        sample_arrivals = otp_results.get('sample_arrivals', [])

        if not sample_arrivals:
            print("No arrival data available")
            return None

        # Convert arrival records to DataFrame format
        records = []
        for arrival in sample_arrivals:
            # Determine if on-time based on thresholds
            diff_sec = arrival['difference_seconds']
            early_thresh = otp_results['thresholds']['early_threshold_seconds']
            late_thresh = otp_results['thresholds']['late_threshold_seconds']
            is_on_time = early_thresh <= diff_sec <= late_thresh

            records.append({
                'timestamp': arrival['actual_time'],
                'vehicle_id': arrival['vehicle_id'],
                'trip_id': arrival['matched_trip_id'],
                'direction_id': None,  # Will look up later
                'lateness_minutes': diff_sec / 60.0,
                'is_on_time': is_on_time,
                'confidence': arrival['match_confidence']
            })

        # Look up direction_id for each trip
        from src.models import Trip
        for record in records:
            if record['trip_id']:
                trip = db.query(Trip).filter(Trip.trip_id == record['trip_id']).first()
                if trip:
                    record['direction_id'] = trip.direction_id

        df = pd.DataFrame(records)

        print(f"\nOTP Summary:")
        print(f"  Total arrivals: {len(df)}")
        print(f"  On-time: {df['is_on_time'].sum()} ({df['is_on_time'].mean()*100:.1f}%)")
        print(f"  Late: {(~df['is_on_time']).sum()} ({(~df['is_on_time']).mean()*100:.1f}%)")
        print(f"  Average lateness: {df['lateness_minutes'].mean():.1f} minutes")

        return df

    finally:
        db.close()


def plot_otp_pie_chart(df: pd.DataFrame, output_path: Path):
    """Create pie chart showing overall OTP rate"""
    fig, ax = plt.subplots(figsize=(8, 8))

    on_time_count = df['is_on_time'].sum()
    late_count = len(df) - on_time_count

    colors = ['#2ecc71', '#e74c3c']  # Green for on-time, red for late
    sizes = [on_time_count, late_count]
    labels = [f'On-Time\n{on_time_count} vehicles\n({on_time_count/len(df)*100:.1f}%)',
              f'Late\n{late_count} vehicles\n({late_count/len(df)*100:.1f}%)']

    ax.pie(sizes, labels=labels, colors=colors, autopct='', startangle=90, textprops={'fontsize': 12})
    ax.set_title(f'On-Time Performance Overview\nTotal: {len(df)} vehicles', fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_path}")
    plt.close()


def plot_otp_over_time(df: pd.DataFrame, output_path: Path):
    """Create line chart showing OTP rate over time"""
    fig, ax = plt.subplots(figsize=(14, 6))

    # Sort by timestamp
    df_sorted = df.sort_values('timestamp')

    # Calculate rolling OTP rate (window of 10 observations)
    df_sorted['otp_rolling'] = df_sorted['is_on_time'].rolling(window=10, min_periods=1).mean() * 100

    # Plot
    ax.plot(df_sorted['timestamp'], df_sorted['otp_rolling'], linewidth=2, color='#3498db')
    ax.axhline(y=100, color='#2ecc71', linestyle='--', linewidth=1, alpha=0.5, label='100% On-Time')
    ax.axhline(y=df['is_on_time'].mean()*100, color='#e67e22', linestyle='--', linewidth=1.5,
               label=f'Average: {df["is_on_time"].mean()*100:.1f}%')

    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('On-Time Performance (%)', fontsize=12)
    ax.set_title('On-Time Performance Over Time\n(10-observation rolling average)', fontsize=14, fontweight='bold')
    ax.set_ylim(0, 105)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    # Rotate x-axis labels
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_path}")
    plt.close()


def plot_lateness_distribution(df: pd.DataFrame, output_path: Path):
    """Create histogram showing distribution of lateness"""
    fig, ax = plt.subplots(figsize=(12, 6))

    # Define OTP window (-1 to +5 minutes)
    otp_min, otp_max = -1, 5

    # Create histogram
    bins = range(int(df['lateness_minutes'].min()) - 1, int(df['lateness_minutes'].max()) + 2, 1)
    counts, edges, patches = ax.hist(df['lateness_minutes'], bins=bins, edgecolor='black', alpha=0.7)

    # Color bars based on OTP window
    for i, patch in enumerate(patches):
        bin_center = (edges[i] + edges[i+1]) / 2
        if otp_min <= bin_center <= otp_max:
            patch.set_facecolor('#2ecc71')  # Green for on-time
        else:
            patch.set_facecolor('#e74c3c')  # Red for late

    # Add vertical lines for OTP window
    ax.axvline(x=otp_min, color='#27ae60', linestyle='--', linewidth=2, label=f'OTP Window ({otp_min} to +{otp_max} min)')
    ax.axvline(x=otp_max, color='#27ae60', linestyle='--', linewidth=2)
    ax.axvline(x=0, color='black', linestyle='-', linewidth=1, alpha=0.5, label='Exactly On Schedule')

    ax.set_xlabel('Lateness (minutes)', fontsize=12)
    ax.set_ylabel('Number of Vehicles', fontsize=12)
    ax.set_title(f'Distribution of Vehicle Lateness\nMean: {df["lateness_minutes"].mean():.1f} min, Median: {df["lateness_minutes"].median():.1f} min',
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_path}")
    plt.close()


def plot_otp_by_direction(df: pd.DataFrame, output_path: Path):
    """Create bar chart showing OTP by direction"""
    fig, ax = plt.subplots(figsize=(10, 6))

    # Calculate OTP by direction
    if 'direction_id' not in df.columns or df['direction_id'].isna().all():
        print("No direction data available, skipping direction plot")
        return

    df_with_dir = df[df['direction_id'].notna()]

    if len(df_with_dir) == 0:
        print("No vehicles with direction data, skipping direction plot")
        return

    otp_by_direction = df_with_dir.groupby('direction_id')['is_on_time'].agg(['sum', 'count', 'mean']).reset_index()
    otp_by_direction['otp_rate'] = otp_by_direction['mean'] * 100
    otp_by_direction['direction_label'] = 'Direction ' + otp_by_direction['direction_id'].astype(str)

    # Create bars
    bars = ax.bar(otp_by_direction['direction_label'], otp_by_direction['otp_rate'],
                  color=['#3498db', '#9b59b6'], edgecolor='black', linewidth=1.5)

    # Add value labels on bars
    for i, (bar, row) in enumerate(zip(bars, otp_by_direction.itertuples())):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + 2,
                f'{height:.1f}%\n({int(row.sum)}/{int(row.count)})',
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    # Add average line
    overall_otp = df_with_dir['is_on_time'].mean() * 100
    ax.axhline(y=overall_otp, color='#e67e22', linestyle='--', linewidth=2,
               label=f'Overall Average: {overall_otp:.1f}%')

    ax.set_ylabel('On-Time Performance (%)', fontsize=12)
    ax.set_title('On-Time Performance by Direction', fontsize=14, fontweight='bold')
    ax.set_ylim(0, 105)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='Visualize on-time performance for a route')
    parser.add_argument('route_id', help='Route ID (e.g., C51, C53)')
    parser.add_argument('--output-dir', default='visualizations/output', help='Output directory for images')

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(f"OTP Visualization for Route {args.route_id}")
    print("=" * 70)

    # Collect data
    df = collect_otp_data(args.route_id)

    if df is None or len(df) == 0:
        print("\nNo data available for visualization")
        sys.exit(1)

    # Generate visualizations
    print("\nGenerating visualizations...")
    print("-" * 70)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    route_clean = args.route_id.replace('/', '_')

    plot_otp_pie_chart(df, output_dir / f"{route_clean}_otp_pie_{timestamp}.png")
    plot_otp_over_time(df, output_dir / f"{route_clean}_otp_over_time_{timestamp}.png")
    plot_lateness_distribution(df, output_dir / f"{route_clean}_lateness_dist_{timestamp}.png")
    plot_otp_by_direction(df, output_dir / f"{route_clean}_otp_by_direction_{timestamp}.png")

    print("-" * 70)
    print(f"\nAll visualizations saved to: {output_dir}")
    print("=" * 70)


if __name__ == "__main__":
    main()
