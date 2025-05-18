import os
import unittest
import pandas as pd
from tempfile import NamedTemporaryFile

from src.utils.label_processor import LabelProcessor

class TestLabelProcessor(unittest.TestCase):
    """Test cases for the LabelProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary ground truth CSV
        self.ground_truth_file = NamedTemporaryFile(delete=False, suffix='.csv')
        
        # Write test data to the CSV that matches the expected format
        ground_truth_data = pd.DataFrame({
            'R.P': ['1.1', '1.1', '1.2', '1.2'],
            'Date': ['', '', '', ''],
            'Rep': ['', '', '', ''],
            'TRT': ['PK', 'PK', 'PK', 'PK'],
            'Path': ['BR', 'DM', 'BR', 'DM'],
            'L1': [95, 85, '-', 90],
            'L2': ['-', 95, '-', '-'],
            'L3': ['-', '-', '-', 25],
            'L4': [35, 95, 35, '-'],
            'L5': [90, '-', 45, 40],
            'L6': ['-', 85, 5, 35],
            'L7': ['-', 55, 50, '-'],
            'L8': [95, 15, 40, 5],
            'L9': [40, '-', 45, 10],
            'L10': [85, 80, 80, 30]
        })
        ground_truth_data.to_csv(self.ground_truth_file.name, index=False)
        
        # Initialize the label processor
        self.processor = LabelProcessor(self.ground_truth_file.name)
    
    def tearDown(self):
        """Tear down test fixtures."""
        os.unlink(self.ground_truth_file.name)
    
    def test_extract_text_from_label(self):
        """Test extracting text from a label value."""
        label_item = {
            'value': [
                {
                    'page': 1,
                    'text': '95',
                    'boundingBoxes': []
                }
            ]
        }
        self.assertEqual(self.processor._extract_text_from_label(label_item), '95')
        
        # Test with empty value
        self.assertEqual(self.processor._extract_text_from_label({}), '')
    
    def test_group_labels_by_rp_path(self):
        """Test grouping labels by R.P and Path."""
        # Create a sample labels data structure
        labels_data = {
            'document': 'test.pdf',
            'labels': [
                {
                    'label': 'dynamic/0/R.P',
                    'value': [{'page': 1, 'text': '1.1', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/0/Path',
                    'value': [{'page': 1, 'text': 'BR', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/0/L1',
                    'value': [{'page': 1, 'text': '95', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/1/R.P',
                    'value': [{'page': 1, 'text': '1.1', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/1/Path',
                    'value': [{'page': 1, 'text': 'DM', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/1/L1',
                    'value': [{'page': 1, 'text': '85', 'boundingBoxes': []}]
                }
            ]
        }
        
        groups = self.processor._group_labels_by_rp_path(labels_data)
        self.assertEqual(len(groups), 2)
        self.assertIn(('1.1', 'BR'), groups)
        self.assertIn(('1.1', 'DM'), groups)
    
    def test_correct_labels(self):
        """Test correcting labels using ground truth."""
        # Create a sample labels data structure with values that need correction
        labels_data = {
            'document': 'test.pdf',
            'labels': [
                {
                    'label': 'dynamic/0/R.P',
                    'value': [{'page': 1, 'text': '1.1', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/0/Path',
                    'value': [{'page': 1, 'text': 'BR', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/0/L1',
                    'value': [{'page': 1, 'text': '50', 'boundingBoxes': []}]  # Wrong value, should be 95
                },
                {
                    'label': 'dynamic/0/L4',
                    'value': [{'page': 1, 'text': '35', 'boundingBoxes': []}]  # Correct value
                },
                {
                    'label': 'dynamic/1/R.P',
                    'value': [{'page': 1, 'text': '1.1', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/1/Path',
                    'value': [{'page': 1, 'text': 'DM', 'boundingBoxes': []}]
                },
                {
                    'label': 'dynamic/1/L1',
                    'value': [{'page': 1, 'text': '85', 'boundingBoxes': []}]  # Correct value
                }
            ]
        }
        
        corrected = self.processor.correct_labels(labels_data)
        
        # Check that L1 for R.P=1.1, Path=BR was corrected to 95
        l1_label = corrected['labels'][2]
        self.assertEqual(l1_label['value'][0]['text'], '95')
        
        # Check that other values remain the same
        l4_label = corrected['labels'][3]
        self.assertEqual(l4_label['value'][0]['text'], '35')
        
        # Check that L1 for R.P=1.1, Path=DM remains the same (already correct)
        l1_dm_label = corrected['labels'][6]
        self.assertEqual(l1_dm_label['value'][0]['text'], '85')
    
    def test_no_matching_ground_truth(self):
        """Test behavior when no matching ground truth is found."""
        # Create a sample labels data with R.P and Path not in ground truth
        labels_data = {
            'document': 'test.pdf',
            'labels': [
                {
                    'label': 'dynamic/0/R.P',
                    'value': [{'page': 1, 'text': '9.9', 'boundingBoxes': []}]  # Not in ground truth
                },
                {
                    'label': 'dynamic/0/Path',
                    'value': [{'page': 1, 'text': 'XX', 'boundingBoxes': []}]  # Not in ground truth
                },
                {
                    'label': 'dynamic/0/L1',
                    'value': [{'page': 1, 'text': '50', 'boundingBoxes': []}]
                }
            ]
        }
        
        # Should return original data unchanged
        corrected = self.processor.correct_labels(labels_data)
        self.assertEqual(corrected['labels'][2]['value'][0]['text'], '50')

if __name__ == '__main__':
    unittest.main() 