#!/usr/bin/env python3
"""
Test script for the simplified biosample extraction.
"""

import tempfile
from pathlib import Path
import sys

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from omicidx_etl.biosample.extract import extract_all, get_file_stats


def test_extraction():
    """Test the extraction with a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir) / "biosample_test"
        
        print(f"Testing extraction to {output_dir}")
        
        try:
            # Test extraction (this will download real data)
            results = extract_all(output_dir)
            
            print("\nExtraction Results:")
            for entity, files in results.items():
                print(f"  {entity}: {len(files)} files")
                for file_path in files:
                    print(f"    - {file_path.name} ({file_path.stat().st_size / 1024 / 1024:.1f} MB)")
            
            # Test stats
            stats = get_file_stats(output_dir)
            print("\nFile Statistics:")
            for entity, info in stats.items():
                if info['file_count'] > 0:
                    print(f"  {entity}: {info['file_count']} files, {info['total_size_mb']:.1f} MB")
            
            print("\n✓ Test completed successfully!")
            return True
            
        except Exception as e:
            print(f"\n✗ Test failed: {e}")
            return False


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    success = test_extraction()
    sys.exit(0 if success else 1)
