import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import logging
from typing import Dict, List, Optional
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreditScoreGenerator:
    """
    Generate realistic Canadian credit score data for banking demo.
    Follows Equifax and TransUnion scoring patterns used in Canada.
    """
    
    def __init__(self):
        self.fake = Faker('en_CA')
        
        # Canadian credit bureaus
        self.bureaus = ['Equifax', 'TransUnion']
        
        # Credit score ranges (Canadian system: 300-900)
        self.score_ranges = {
            'Excellent': (750, 900),
            'Very Good': (720, 749),
            'Good': (650, 719),
            'Fair': (560, 649),
            'Poor': (300, 559)
        }
        
        # Risk categories based on Canadian banking standards
        self.risk_categories = {
            (800, 900): 'VERY_LOW',
            (750, 799): 'LOW',
            (650, 749): 'MEDIUM',
            (600, 649): 'MEDIUM_HIGH',
            (550, 599): 'HIGH',
            (300, 549): 'VERY_HIGH'
        }
    
    def get_risk_category(self, credit_score: int) -> str:
        """Determine risk category based on credit score."""
        for (min_score, max_score), category in self.risk_categories.items():
            if min_score <= credit_score <= max_score:
                return category
        return 'UNKNOWN'
    
    def generate_realistic_score(self, customer_segment: str, customer_age: int, 
                                customer_tenure_years: int) -> int:
        """
        Generate realistic credit score based on customer characteristics.
        Uses demographic patterns from Canadian credit data.
        """
        
        # Base score influenced by customer segment
        if customer_segment == 'Senior Banking':
            base_score = random.randint(680, 820)  # Seniors typically have better credit
        elif customer_segment == 'Established Banking':
            base_score = random.randint(640, 780)  # Middle-aged, established
        elif customer_segment == 'Prime Banking':
            base_score = random.randint(600, 750)  # Working age, variable credit
        elif customer_segment == 'Young Professional':
            base_score = random.randint(550, 720)  # Younger, building credit
        else:
            base_score = random.randint(500, 700)  # Default range
        
        # Age adjustments (older typically means better credit history)
        if customer_age >= 65:
            age_adjustment = random.randint(0, 40)
        elif customer_age >= 45:
            age_adjustment = random.randint(-10, 30)
        elif customer_age >= 30:
            age_adjustment = random.randint(-20, 20)
        else:
            age_adjustment = random.randint(-30, 10)
        
        # Tenure adjustments (longer banking relationship = better credit)
        if customer_tenure_years >= 10:
            tenure_adjustment = random.randint(10, 30)
        elif customer_tenure_years >= 5:
            tenure_adjustment = random.randint(0, 20)
        elif customer_tenure_years >= 2:
            tenure_adjustment = random.randint(-10, 10)
        else:
            tenure_adjustment = random.randint(-20, 5)
        
        # Calculate final score with adjustments
        final_score = base_score + age_adjustment + tenure_adjustment
        
        # Ensure score is within valid range (300-900)
        final_score = max(300, min(900, final_score))
        
        # Add some random variation for realism
        variation = random.randint(-15, 15)
        final_score = max(300, min(900, final_score + variation))
        
        return final_score
    
    def generate_historical_scores(self, customer_id: str, current_score: int, 
                                 months_back: int = 24) -> List[Dict]:
        """Generate historical credit scores showing progression over time."""
        
        historical_scores = []
        
        for month_offset in range(months_back, 0, -1):
            score_date = datetime.now().date() - timedelta(days=month_offset * 30)
            
            # Historical scores tend to trend toward current score
            if month_offset > 12:
                # Older scores have more variation
                historical_score = current_score + random.randint(-50, 50)
            elif month_offset > 6:
                # Recent history closer to current
                historical_score = current_score + random.randint(-25, 25)
            else:
                # Very recent scores very close to current
                historical_score = current_score + random.randint(-10, 10)
            
            # Ensure valid range
            historical_score = max(300, min(900, historical_score))
            
            # Random bureau (sometimes customers have scores from both)
            bureau = random.choice(self.bureaus)
            
            historical_scores.append({
                'customer_id': customer_id,
                'bureau_name': bureau,
                'credit_score': historical_score,
                'score_date': score_date,
                'risk_category': self.get_risk_category(historical_score),
                'score_type': 'Historical'
            })
        
        return historical_scores
    
    def generate_credit_scores_for_customers(self, customers_df: pd.DataFrame, 
                                           include_historical: bool = True) -> pd.DataFrame:
        """
        Generate credit scores for existing customer base.
        
        Args:
            customers_df: DataFrame containing customer information
            include_historical: Whether to include historical score data
            
        Returns:
            DataFrame with credit score data
        """
        logger.info(f"Generating credit scores for {len(customers_df)} customers...")
        
        all_credit_scores = []
        
        for index, customer in customers_df.iterrows():
            customer_id = customer['customer_id']
            customer_segment = customer.get('customer_segment', 'Unknown')
            customer_age = customer.get('age', 35)
            customer_tenure_years = customer.get('tenure_years', 2)
            
            # Generate current credit score
            current_score = self.generate_realistic_score(
                customer_segment, customer_age, customer_tenure_years
            )
            
            # Current scores from both bureaus 
            for bureau in self.bureaus:
                bureau_score = current_score + random.randint(-10, 10)
                bureau_score = max(300, min(900, bureau_score))
                
                credit_record = {
                    'customer_id': customer_id,
                    'bureau_name': bureau,
                    'credit_score': bureau_score,
                    'score_date': datetime.now().date(),
                    'risk_category': self.get_risk_category(bureau_score),
                    'score_type': 'Current'
                }
                all_credit_scores.append(credit_record)
            
            # Add historical data if requested
            if include_historical:
                historical_scores = self.generate_historical_scores(
                    customer_id, current_score, months_back=24
                )
                all_credit_scores.extend(historical_scores)
            
            if (index + 1) % 1000 == 0:
                logger.info(f"Generated credit scores for {index + 1} customers...")
        
        credit_df = pd.DataFrame(all_credit_scores)
        
        # Add additional realistic fields
        credit_df['loaded_at'] = datetime.now()
        credit_df['data_source'] = 'Generated'
        credit_df['score_model'] = 'FICO_8'  
        
        logger.info(f"Credit score generation complete! Generated {len(credit_df)} records")
        
        return credit_df
    
    def add_credit_events(self, credit_df: pd.DataFrame) -> pd.DataFrame:
        """Add realistic credit events that might affect scores."""
        
        # Create some customers with credit events
        unique_customers = credit_df['customer_id'].unique()
        customers_with_events = random.sample(
            list(unique_customers), 
            min(len(unique_customers) // 10, 500)  # 10% of customers or max 500
        )
        
        credit_events = []
        
        for customer_id in customers_with_events:
            event_date = self.fake.date_between(start_date='-2y', end_date='today')
            
            event_types = [
                'NEW_CREDIT_ACCOUNT',
                'MISSED_PAYMENT',
                'CREDIT_LIMIT_INCREASE',
                'ACCOUNT_CLOSURE',
                'CREDIT_INQUIRY'
            ]
            
            event = {
                'customer_id': customer_id,
                'event_type': random.choice(event_types),
                'event_date': event_date,
                'impact_score': random.randint(-50, 25),
                'description': f"Credit event recorded on {event_date}"
            }
            credit_events.append(event)
        
        events_df = pd.DataFrame(credit_events)
        return events_df
    
    def generate_credit_insights(self, credit_df: pd.DataFrame) -> Dict:
        """Generate insights about the credit score dataset."""
        
        current_scores = credit_df[credit_df['score_type'] == 'Current']
        
        insights = {
            'total_records': len(credit_df),
            'unique_customers': credit_df['customer_id'].nunique(),
            'bureaus_covered': list(credit_df['bureau_name'].unique()),
            'score_distribution': {
                'mean': current_scores['credit_score'].mean(),
                'median': current_scores['credit_score'].median(),
                'std': current_scores['credit_score'].std(),
                'min': current_scores['credit_score'].min(),
                'max': current_scores['credit_score'].max()
            },
            'risk_category_distribution': current_scores['risk_category'].value_counts().to_dict(),
            'date_range': {
                'earliest': credit_df['score_date'].min(),
                'latest': credit_df['score_date'].max()
            }
        }
        
        return insights
    
    def save_to_csv(self, credit_df: pd.DataFrame, filename: str = 'credit_scores_sample.csv'):
        """Save credit score data to CSV file."""
        credit_df.to_csv(f'data/sample/{filename}', index=False)
        logger.info(f"Credit score data saved to data/sample/{filename}")
    
    def save_insights(self, insights: Dict, filename: str = 'credit_score_insights.yaml'):
        """Save insights to YAML file."""
        with open(f'data/schemas/{filename}', 'w') as f:
            yaml.dump(insights, f, default_flow_style=False)
        logger.info(f"Credit score insights saved to data/schemas/{filename}")


def main():
    """Main function to generate credit score data."""
    
    # Load existing customer data
    try:
        customers_df = pd.read_csv('data/sample/customers_sample.csv')
        logger.info(f"Loaded {len(customers_df)} customers from existing data")
    except FileNotFoundError:
        logger.error("Customer data not found. Please run generate_customer_data.py first.")
        return
    
    # Initialize credit score generator
    generator = CreditScoreGenerator()
    
    # Generate credit scores
    credit_scores_df = generator.generate_credit_scores_for_customers(
        customers_df, 
        include_historical=True
    )
    
    # Generate credit events
    credit_events_df = generator.add_credit_events(credit_scores_df)
    
    # Generate insights
    insights = generator.generate_credit_insights(credit_scores_df)
    
    # Save all data
    generator.save_to_csv(credit_scores_df, 'credit_scores_sample.csv')
    generator.save_to_csv(credit_events_df, 'credit_events_sample.csv')
    generator.save_insights(insights)

if __name__ == "__main__":
    main()