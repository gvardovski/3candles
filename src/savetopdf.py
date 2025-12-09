import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

def save_backtesting_results_to_pdf(pf, file_path):
    os.makedirs("data/", exist_ok=True)

    stats_df = pf.stats().to_frame()

    with PdfPages(f"{file_path}_stats.pdf") as pdf:
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

    print(f"CSV file with statistics '{f"{file_path}_stats.pdf"}' was created!")