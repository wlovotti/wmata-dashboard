"""
Test OTP calculation using BusPositions deviation data
"""
from src.database import get_session
from src.analytics import calculate_otp_from_bus_positions

db = get_session()

try:
    print("=" * 70)
    print("Testing OTP Calculation from BusPositions API")
    print("=" * 70)

    # Calculate OTP for C51 using the 8 positions we collected earlier
    result = calculate_otp_from_bus_positions(db, 'C51')

    print(f"\nRoute: {result['route_id']}")
    print(f"Data Source: {result['data_source']}")
    print(f"Observations: {result['observations']}")
    print(f"Unique Vehicles: {result['unique_vehicles']}")
    print(f"\nOTP Metrics:")
    print(f"  On-time: {result['on_time_percentage']}%")
    print(f"  Early:   {result['early_percentage']}%")
    print(f"  Late:    {result['late_percentage']}%")
    print(f"\nAverage Deviation: {result['avg_deviation_minutes']} minutes")

    print("\n" + "=" * 70)
    print("Benefits of BusPositions API:")
    print("=" * 70)
    print("✓ No trip matching required")
    print("✓ No stop proximity calculations")
    print("✓ No schedule time parsing")
    print("✓ Direct deviation data from WMATA")
    print("✓ Much simpler and faster!")

finally:
    db.close()
