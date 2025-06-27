import os
import re
import streamlit as st
import pandas as pd
import gspread
from io import BytesIO, StringIO
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor

# --- Конфигурация ---
class AppConfig:
    PAGE_TITLE = "🔍 Поиск по Google Таблице"
    PAGE_LAYOUT = "wide"
    CREDENTIALS_FILE = "parsing.json"
    SCOPE = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    # Пароль для доступа к приложению
    PASSWORD = "admin123"  # Замените на ваш реальный пароль

# --- Работа с Google Sheets ---
class GoogleSheetsConnector:
    @staticmethod
    @st.cache_resource
    def get_client():
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            AppConfig.CREDENTIALS_FILE,
            AppConfig.SCOPE
        )
        return gspread.authorize(creds)

    @staticmethod
    def extract_sheet_id(url):
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        return match.group(1) if match else None

# --- Обработка данных ---
class DataProcessor:

    @staticmethod
    def load_worksheet(ws):
        try:
            data = ws.get_all_values()  # считываем ВСЁ как строки
            if not data or len(data) < 2:
                return None

            df = pd.DataFrame(data[1:], columns=data[0])  # пропускаем первую строку (заголовки)
            df['Лист'] = ws.title

            for col in df.columns:
                df[col] = df[col].astype(str).str.strip()

            return df
        except Exception as e:
            print(f"Ошибка загрузки листа '{ws.title}': {e}")
            return None
        

    @staticmethod
    def normalize_text(text):
        text = str(text).lower()

        replacements = {
            'х': 'x',
            '–': '-',
            '—': '-',
            'ё': 'е',
            'мм2': 'мм²',
            'мм^2': 'мм²',
            'см2': 'см²',
            'см^2': 'см²',
            'м2': 'м²',
            'м^2': 'м²',
        }

        for k, v in replacements.items():
            text = text.replace(k, v)

        # Удаляем лишние пробелы между числом и размерностью
        text = re.sub(r'(?<=\d)\s+(?=мм²|см²|м²)', '', text)

        return text

    @staticmethod
    def split_preserve_sizes(text):
        text = DataProcessor.normalize_text(text)

        # Соединяем размеры: 2 x 2.5 => 2x2.5
        text = re.sub(
            r'(\d+(?:[.,]\d+)?)\s*[xх×*]\s*(\d+(?:[.,]\d+)?)',
            r'\1x\2',
            text
        )

        # Преобразуем разделённые единицы измерения: "мм 2" -> "мм²", "см^2" -> "см²", и т.д.
        text = re.sub(r'\bмм\s*[\^]?\s*2\b', 'мм²', text)
        text = re.sub(r'\bсм\s*[\^]?\s*2\b', 'см²', text)
        text = re.sub(r'\bм\s*[\^]?\s*2\b', 'м²', text)

        # Разбиваем строку на слова, отдельно выделяя размер и единицу измерения
        matches = re.findall(r'\d+(?:[.,]\d+)?x\d+(?:[.,]\d+)?|мм²|см²|м²|\w+', text)

        return matches

    @staticmethod
    def match_query(row_text, query_words, require_all=False):
        row_words = DataProcessor.split_preserve_sizes(row_text)
        match_count = sum(1 for word in query_words if word in row_words)
        if require_all:
            return match_count if match_count == len(query_words) else 0
        return match_count

# --- UI ---
class UIComponents:
    @staticmethod
    def setup_page():
        st.set_page_config(
            page_title=AppConfig.PAGE_TITLE,
            layout=AppConfig.PAGE_LAYOUT
        )
        st.title(AppConfig.PAGE_TITLE)


    @staticmethod
    def show_results(results, selected_columns):
        if not results.empty:
            results = results.reset_index(drop=True)
            results.index = results.index + 2
            results.index.name = "№ строки"

            results["№ строки"] = results.index
            if selected_columns:
                filtered_results = results[[col for col in selected_columns if col in results.columns]]
            else:
                filtered_results = results

            # Создаем копию для HTML-отображения
            html_df = filtered_results.copy()
            if "URL" in html_df.columns:
                html_df["URL"] = html_df["URL"].apply(
                    lambda x: f'<a href="{x}" target="_blank">{x}</a>' if x.startswith("http") else x
                )

            # Показываем таблицу в виде HTML
            st.markdown(
                html_df.to_html(escape=False, index=False),
                unsafe_allow_html=True
            )

            # Скачивание (оригинальные данные, без HTML-тегов)
            excel_buffer = BytesIO()
            csv_buffer = StringIO()

            filtered_results.to_excel(excel_buffer, index=False, engine='openpyxl')
            filtered_results.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

            st.download_button(
                label="⬇️ Скачать результаты в Excel",
                data=excel_buffer.getvalue(),
                file_name="results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.download_button(
                label="⬇️ Скачать результаты в CSV",
                data=csv_buffer.getvalue(),
                file_name="results.csv",
                mime="text/csv"
            )


# --- Основное приложение ---
class GoogleSheetSearchApp:
    def __init__(self):
        UIComponents.setup_page()
        self.client = GoogleSheetsConnector.get_client()
        self.initialize_session_state()
        self.authenticate()

    def initialize_session_state(self):
        if 'combined_df' not in st.session_state:
            st.session_state.combined_df = None
        if 'sheet_id' not in st.session_state:
            st.session_state.sheet_id = None
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False

    def authenticate(self):
        if not st.session_state.authenticated:
            # Создаем 4 колонки, поле ввода будет в первой (1/4 ширины)
            col1, _, _, _ = st.columns([1, 1, 1, 1])
            
            with col1:
                password = st.text_input("🔒 Введите пароль для доступа", 
                                    type="password",
                                    key="password_input")
                
                if st.button("Войти", key="login_button"):
                    if password == AppConfig.PASSWORD:
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error("❌ Неверный пароль")
            return
        
        # Если аутентификация пройдена, показываем основной функционал
        self.show_main_app()

    def process_sheets(self, spreadsheet):
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(DataProcessor.load_worksheet, spreadsheet.worksheets()))
        return [df for df in dfs if df is not None]

    def load_data(self, sheet_url):
        sheet_id = GoogleSheetsConnector.extract_sheet_id(sheet_url)
        if not sheet_id:
            st.error("❌ Некорректная ссылка на Google Таблицу")
            return False

        if st.session_state.sheet_id != sheet_id:
            try:
                spreadsheet = self.client.open_by_key(sheet_id)
                all_data = self.process_sheets(spreadsheet)

                if not all_data:
                    st.warning("⚠️ В таблице нет данных")
                    return False

                st.session_state.combined_df = pd.concat(all_data, ignore_index=True)
                st.session_state.sheet_id = sheet_id
                st.success("✅ Данные успешно загружены")
                return True
            except Exception as e:
                st.error(f"❌ Ошибка загрузки данных: {str(e)}")
                return False
        return True

    def show_main_app(self):
        sheet_url = st.text_input("📎 Вставьте ссылку на Google Таблицу", key="sheet_url")
        
        if sheet_url:
            if st.button("🔄 Загрузить данные") or st.session_state.combined_df is not None:
                if self.load_data(sheet_url) and st.session_state.combined_df is not None:
                    combined_df = st.session_state.combined_df

                    column = st.selectbox(
                        "📁 Выберите колонку для поиска",
                        combined_df.columns,
                        key="search_column"
                    )

                    all_columns = ['№ строки', 'Лист'] + [col for col in combined_df.columns if col != 'Лист']
                    selected_columns = st.multiselect(
                        "📋 Выберите колонки для вывода",
                        options=all_columns,
                        default=[],
                        key="output_columns",
                        help="Если не выбрано ни одной, будут отображены все доступные колонки."
                    )

                    search_query = st.text_input("🔎 Введите слово или часть слова для поиска", key="search_query")

                    exact_match = st.checkbox("🧩 Только полное совпадение всех слов", value=True)
                    partial_match = st.checkbox("🔍 Частичное совпадение")

                    if exact_match and partial_match:
                        st.warning("⚠️ Выберите только один метод поиска")
                        return

                    if search_query:
                        query_words = DataProcessor.split_preserve_sizes(search_query)

                        require_all = exact_match and not partial_match
                        combined_df = combined_df.copy()
                        combined_df['__match_count'] = combined_df[column].apply(
                            lambda text: DataProcessor.match_query(text, query_words, require_all=require_all)
                        )

                        results = combined_df[combined_df['__match_count'] > 0]
                        results = results.sort_values(by='__match_count', ascending=False)
                        results = results.drop(columns='__match_count')

                        st.success(f"🔎 Найдено: {len(results)} записей")
                        UIComponents.show_results(results, selected_columns)

# --- Запуск приложения ---
if __name__ == "__main__":
    GoogleSheetSearchApp()