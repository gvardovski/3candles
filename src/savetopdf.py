import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

def save_backtesting_results_to_pdf(pf):
    output_dir = "data/output/"
    os.makedirs(output_dir, exist_ok=True)

    stats = pf.stats()
    stats_df = stats.to_frame()

    with PdfPages(f"{output_dir}/portfolio_report.pdf") as pdf:
        fig, ax = plt.subplots(figsize=(8.5, len(stats_df) * 0.4))
        ax.axis("off")
        table = ax.table(
            cellText=stats_df.values,
            colLabels=stats_df.columns,
            rowLabels=stats_df.index,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.5)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close()