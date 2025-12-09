import matplotlib.pyplot as plt
import seaborn as sns
import os

def create_heatmap(results_df, metric_name, output_dir='data/', figsize=(30, 30)):
    try:
        print(f"\n>>> Creating heatmap for: {metric_name}")
        
        # Check if metric exists in dataframe
        if metric_name not in results_df.columns:
            print(f"WARNING: '{metric_name}' not found in results. Skipping...")
            return False
        
        # Create pivot table
        heatmap_data = results_df.pivot_table(
            index='SL', 
            columns='TP', 
            values=metric_name, 
            aggfunc='mean'
        )
        
        # Check if heatmap has data
        if heatmap_data.empty:
            print(f"WARNING: No data for '{metric_name}'. Skipping...")
            return False
        
        # Check for all NaN values
        if heatmap_data.isna().all().all():
            print(f"WARNING: All values are NaN for '{metric_name}'. Skipping...")
            return False
        
        # Print statistics
        valid_values = heatmap_data.stack().dropna()
        print(f"  Valid values: {len(valid_values)}")
        print(f"  Min: {valid_values.min():.2f}, Max: {valid_values.max():.2f}, Mean: {valid_values.mean():.2f}")
        
        # Create and save heatmap
        plt.figure(figsize=figsize)
        sns.heatmap(
            heatmap_data, 
            annot=True, 
            fmt=".2f", 
            cmap='coolwarm', 
            center=0,
            cbar_kws={'label': metric_name}
        )
        plt.title(f"{metric_name} Heatmap")
        plt.xlabel("WICK_RATIO")
        plt.ylabel("OP_WICK_RATIO")
        plt.tight_layout()
        
        os.makedirs(output_dir, exist_ok=True)
        filepath = f"{output_dir}/{metric_name}.png"
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Saved: {filepath}")
        return True
        
    except Exception as e:
        print(f"ERROR creating heatmap for '{metric_name}': {e}")
        plt.close()
        return False