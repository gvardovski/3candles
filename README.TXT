## Installation Process (Linux)

1. **Install Poetry**  
   Run the following command in your terminal to install Poetry:
   ```bash
   sudo apt install poetry
   ```
   If Poetry is already installed on your system, you can skip this step.

2. **Install Dependencies and Set Up Virtual Environment**  
   In the root directory of your project, execute:
   ```bash
   poetry install
   ```
   This will install a virtual environment and all dependencies.

3. **Create `.env` File**  
   Run:
   ```bash
   echo -e "IS_JWT_USER=\nIS_JWT=\n3CANDLES_DATA_CONFIG_PATH=configs/dataconfig.yaml\n3CANDLES_CONFIG_PATH=configs/config.yaml" | tee .env >/dev/null
   ```
   This command creates the `.env` file and fills it with initial data configuration. 
   If you skip this step, it will be created automatically.

4. **Edit `.env` File (Optional)**  
   Fill in the `IS_JWT=` value in `.env`. Also, edit the config paths if needed.

5. **Edit Config Files (Optional)**  
   Edit `config.yaml` and `dataconfig.yaml` in the `configs/` folder if you need to adjust settings.

6. **Run the Program**  
   Execute:
   ```bash
   poetry run python3 main.py
   ```

## Installation Process (Windows)

1. **Install Poetry**  
   Download and install Poetry from the official documentation here:  
   [https://python-poetry.org/docs/#installation](https://python-poetry.org/docs/#installation)  
   You may use:
   ```powershell
   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
   ```
   If Poetry is already installed on your system, you can skip this step.

2. **Install Dependencies and Set Up Virtual Environment**  
   Open a terminal in the root directory of your project and run:
   ```powershell
   poetry install
   ```
   This will install a virtual environment and all dependencies.

3. **Create `.env` File**  
   Run the following command in your terminal (PowerShell recommended):
   ```powershell
   @"
IS_JWT_USER=
IS_JWT=
3CANDLES_DATA_CONFIG_PATH=configs/dataconfig.yaml
3CANDLES_CONFIG_PATH=configs/config.yaml
"@ | Out-File -Encoding utf8 .env
   ```
   This will create the `.env` file and fill it with initial data configuration.  
   If you skip this step, it will be created automatically.
   
4. **Edit `.env` File (Optional)**  
   Fill in the `IS_JWT=` value in `.env`. Also, edit the config paths if needed.

5. **Edit Config Files (Optional)**  
   Edit `config.yaml` and `dataconfig.yaml` in the `configs\` folder if you need to adjust settings.

6. **Run the Program**  
   Execute:
   ```powershell
   poetry run python main.py
   ```
