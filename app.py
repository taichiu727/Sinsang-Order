import streamlit as st
import pandas as pd
from datetime import datetime
import os
import io
from PIL import Image
import openpyxl
import tempfile
import base64
from sqlalchemy import create_engine, text
import json
import time
import numpy as np

# Database configuration
DATABASE_URL = "postgresql://neondb_owner:npg_WEzy0a1pQMAl@ep-delicate-forest-a1jhkrwa-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# Initialize database connection
@st.cache_resource
def init_db():
    return create_engine(DATABASE_URL)

def extract_floating_images(excel_file):
    """Extract floating images from Excel file and match them to rows"""
    images_data = []
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
        tmp_file.write(excel_file.getvalue())
        tmp_path = tmp_file.name

    try:
        wb = openpyxl.load_workbook(tmp_path)
        ws = wb.active

        for drawing in ws._images:
            row_idx = drawing.anchor._from.row
            img_data = drawing._data()
            try:
                image = Image.open(io.BytesIO(img_data))
                image.thumbnail((300, 300))
                buffered = io.BytesIO()
                image.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode()
                images_data.append({
                    'row_idx': row_idx,
                    'image_data': f'data:image/png;base64,{img_str}'
                })
            except Exception as e:
                st.error(f"Error processing image: {str(e)}")
                continue

        os.unlink(tmp_path)
        return images_data

    except Exception as e:
        st.error(f"Error extracting images: {str(e)}")
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return []

def create_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                batch_number INTEGER,
                upload_time TIMESTAMP,
                stall_number TEXT,
                store_name TEXT,
                product_name TEXT,
                color_size TEXT,
                quantity INTEGER,
                received BOOLEAN DEFAULT FALSE,
                missing_quantity INTEGER DEFAULT 0,
                notes TEXT,
                image_data TEXT,
                original_data JSONB
            )
        """))
        conn.commit()

@st.fragment
def save_to_db(engine, df):
    def json_serializable_converter(obj):
        """Convert non-serializable objects to JSON-friendly format"""
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, (np.integer, np.floating)):
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        return obj

    with engine.connect() as conn:
        for _, row in df.iterrows():
            row_dict = row.copy()
            
            # Handle different possible column names for upload time
            upload_time_columns = ['上傳時間/업로드 시간', '上傳時間/업로드 時間']
            upload_time_col = next((col for col in upload_time_columns if col in row_dict), None)
            
            if upload_time_col:
                if isinstance(row_dict[upload_time_col], pd.Timestamp):
                    row_dict[upload_time_col] = row_dict[upload_time_col].strftime('%Y-%m-%d %H:%M:%S')
            else:
                # If no upload time column exists, use current time
                row_dict['上傳時間/업로드 시间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert to JSON-serializable dictionary
            original_data = {k: json_serializable_converter(v) for k, v in row_dict.to_dict().items()}
            
            conn.execute(
                text("""
                    INSERT INTO orders (
                        batch_number, upload_time, stall_number, store_name, 
                        product_name, color_size, quantity, received, 
                        missing_quantity, notes, image_data, original_data
                    ) VALUES (
                        :batch_number, :upload_time, :stall_number, :store_name,
                        :product_name, :color_size, :quantity, :received,
                        :missing_quantity, :notes, :image_data, :original_data
                    )
                """),
                {
                    "batch_number": row.get('批次號碼/배치 번호'),
                    "upload_time": pd.to_datetime(row_dict.get('上傳時間/업로드 시间', datetime.now())),
                    "stall_number": row.get('檔口'),
                    "store_name": row.get('店名'),
                    "product_name": row.get('品名'),
                    "color_size": row.get('顏色/尺寸'),
                    "quantity": row.get('數量'),
                    "received": row.get('到貨/입고', False),
                    "missing_quantity": row.get('缺貨數量/부족 수량', 0),
                    "notes": row.get('備註'),
                    "image_data": row.get('照片'),
                    "original_data": json.dumps(original_data)
                }
            )
        conn.commit()

@st.fragment
def load_from_db(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                batch_number as "批次號碼/배치 번호",
                upload_time as "上傳時間/업로드 시間",
                stall_number as "檔口",
                store_name as "店名",
                product_name as "品名",
                color_size as "顏色/尺寸",
                quantity as "數量",
                received as "到貨/입고",
                missing_quantity as "缺貨數量/부족 수량",
                notes as "備註",
                image_data as "照片"
            FROM orders 
            ORDER BY batch_number, upload_time
        """))
        
        df = pd.DataFrame(result.fetchall())
        
        if not df.empty:
            df['上傳時間/업로드 시間'] = pd.to_datetime(df['上傳時間/업로드 시間'])
            
            column_order = [
                '批次號碼/배치 번호',
                '上傳時間/업로드 시間',
                '檔口',
                '店名',
                '品名',
                '顏色/尺寸',
                '數量',
                '到貨/입고',
                '缺貨數量/부족 수량',
                '備註',
                '照片'
            ]
            
            df = df[column_order]
            
        return df

@st.fragment
def batch_update_db(engine, changes_dict, base_df):
    """Perform batch updates to the database efficiently"""
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                update_stmt = text("""
                    UPDATE orders 
                    SET received = :received,
                        missing_quantity = :missing_quantity,
                        notes = :notes
                    WHERE batch_number = :batch_number 
                    AND stall_number = :stall_number
                    AND store_name = :store_name
                    AND product_name = :product_name
                    AND color_size = :color_size
                    AND quantity = :quantity
                """)
                
                for row_idx, changes in changes_dict.items():
                    row_idx = int(row_idx)
                    row = base_df.iloc[row_idx]
                    
                    params = {
                        "batch_number": int(row['批次號碼/배치 번호']),
                        "stall_number": str(row['檔口']),
                        "store_name": str(row['店名']),
                        "product_name": str(row['品名']),
                        "color_size": str(row['顏色/尺寸']),
                        "quantity": int(row['數量']),
                        "received": bool(changes.get('到貨/입고', row['到貨/입고'])),
                        "missing_quantity": int(changes.get('缺貨數量/부족 수량', row['缺貨數量/부족 수량']) or 0),
                        "notes": str(changes.get('備註', row['備註']) or '')
                    }
                    
                    conn.execute(update_stmt, params)
                
                trans.commit()
                return True
                
            except Exception as e:
                trans.rollback()
                st.error(f"Transaction error: {str(e)}")
                return False
                
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False

def initialize_session_state(engine):
    if "viewport_height" not in st.session_state:
        st.session_state.viewport_height = 800
    if "state" not in st.session_state:
        st.session_state.state = {
            "all_orders_df": load_from_db(engine),
            "latest_upload_df": None,
            "boxes": [],
            "edited_df": None
        }
    if "orders_need_refresh" not in st.session_state:
        st.session_state.orders_need_refresh = True
    if "last_edited_df" not in st.session_state:
        st.session_state.last_edited_df = None
    if "pending_changes" not in st.session_state:
        st.session_state.pending_changes = {}

def handle_data_editor_changes(edited_df, engine):
    """Handle changes made in the data editor"""
    # Dynamically get column names for comparison
    edit_columns = ['到貨/입고', '缺貨數量/부족 수량', '備註']
    
    # Ensure all required columns exist
    for col in edit_columns:
        if col not in edited_df.columns:
            st.error(f"Column {col} not found in the DataFrame")
            return st.session_state.last_edited_df

    if st.session_state.last_edited_df is not None:
        changes = []
        for idx, row in edited_df.iterrows():
            last_row = st.session_state.last_edited_df.iloc[idx]
            
            # Check if any of the specific columns have changed
            if any(row[col] != last_row[col] for col in edit_columns):
                changes.append((
                    str(row["批次號碼/배치 번호"]),
                    str(row["檔口"]),
                    str(row["店名"]),
                    str(row["品名"]),
                    str(row["顏色/尺寸"]),
                    int(row["數量"]),
                    bool(row["到貨/입고"]),
                    int(row["缺貨數量/부족 수량"]) if pd.notna(row["缺貨數量/부족 수량"]) else 0,
                    str(row["備註"]) if pd.notna(row["備註"]) else ""
                ))
        
        if changes:
            with st.spinner('Saving changes...'):
                try:
                    with init_db().connect() as conn:
                        trans = conn.begin()
                        try:
                            update_stmt = text("""
                                UPDATE orders 
                                SET received = :received,
                                    missing_quantity = :missing_quantity,
                                    notes = :notes
                                WHERE batch_number = :batch_number 
                                AND stall_number = :stall_number
                                AND store_name = :store_name
                                AND product_name = :product_name
                                AND color_size = :color_size
                                AND quantity = :quantity
                            """)
                            
                            for change in changes:
                                params = {
                                    "batch_number": change[0],
                                    "stall_number": change[1],
                                    "store_name": change[2],
                                    "product_name": change[3],
                                    "color_size": change[4],
                                    "quantity": change[5],
                                    "received": change[6],
                                    "missing_quantity": change[7],
                                    "notes": change[8]
                                }
                                conn.execute(update_stmt, params)
                            
                            trans.commit()
                            st.session_state.last_edited_df = edited_df.copy()
                            st.toast("✅ Changes saved!")
                            return edited_df
                        except Exception as e:
                            trans.rollback()
                            st.error(f"Error saving changes: {str(e)}")
                            return st.session_state.last_edited_df
                except Exception as e:
                    st.error(f"Database connection error: {str(e)}")
                    return st.session_state.last_edited_df
    else:
        st.session_state.last_edited_df = edited_df.copy()
    return edited_df

@st.fragment
def orders_editor_section(engine):
    column_config = {
        "照片": st.column_config.ImageColumn("照片", help="商品圖片", width="medium"),
        "到貨/입고": st.column_config.CheckboxColumn("到貨/입고", help="收貨確認"),
        "缺貨數量/부족 수량": st.column_config.NumberColumn("缺貨數量/부족 수량", help="缺貨數量", min_value=0),
        "備註": st.column_config.TextColumn("備註", help="備註", width="large")
    }

    def handle_edit():
        if 'edited_rows' not in st.session_state.orders_editor:
            return

        edited_rows = st.session_state.orders_editor['edited_rows']
        if not edited_rows:
            return

        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    update_stmt = text("""
                        UPDATE orders 
                        SET received = :received,
                            missing_quantity = :missing_quantity,
                            notes = :notes
                        WHERE batch_number = :batch_number 
                        AND stall_number = :stall_number
                        AND store_name = :store_name
                        AND product_name = :product_name
                        AND color_size = :color_size
                        AND quantity = :quantity
                    """)
                    
                    for row_idx, row_changes in edited_rows.items():
                        row_idx = int(row_idx)
                        row = st.session_state.state["all_orders_df"].iloc[row_idx]
                        
                        params = {
                            "batch_number": int(row['批次號碼/배치 번호']),
                            "stall_number": str(row['檔口']),
                            "store_name": str(row['店名']),
                            "product_name": str(row['品名']),
                            "color_size": str(row['顏色/尺寸']),
                            "quantity": int(row['數量']),
                            "received": bool(row_changes.get('到貨/입고', row['到貨/입고'])),
                            "missing_quantity": int(row_changes.get('缺貨數量/부족 수량', row['缺貨數量/부족 수량']) or 0),
                            "notes": str(row_changes.get('備註', row['備註']) or '')
                        }
                        
                        conn.execute(update_stmt, params)
                    
                    trans.commit()
                    
                 
                
                except Exception as e:
                    trans.rollback()
                    st.error(f"Error saving changes: {str(e)}")
        
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")

    # Render the data editor with on_change callback
    return st.data_editor(
        st.session_state.state["all_orders_df"],
        column_config=column_config,
        key="orders_editor",
        num_rows="fixed",
        height=st.session_state.viewport_height,
        disabled=[
            "批次號碼/배치 번호", 
            "上傳時間/업로드 시間", 
            "檔口", 
            "店名", 
            "品名", 
            "顏色/尺寸", 
            "數量", 
            "照片"
        ],
        on_change=handle_edit
    )

@st.fragment
def display_order_statistics(df):
    """Display order statistics"""
    if not df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_orders = len(df)
            received_orders = df['到貨/입고'].sum()
            completion_rate = (received_orders/total_orders*100) if total_orders > 0 else 0
            st.metric("訂單完成率 / 주문 완료율", 
                     f"{completion_rate:.1f}%",
                     f"{received_orders}/{total_orders}")
        
        with col2:
            total_missing = df['缺貨數量/부족 수량'].sum()
            st.metric("總缺貨數量 / 총 부족 수량", f"{total_missing}")
        
        with col3:
            pending_orders = total_orders - received_orders
            st.metric("待處理訂單 / 처리 대기 주문", f"{pending_orders}")
        
        with col4:
            total_batches = df['批次號碼/배치 번호'].nunique()
            st.metric("總批次數 / 총 배치 수", f"{total_batches}")



@st.fragment
def render_upload_tab(engine):
    st.header('上傳訂單 / 주문 업로드')
    uploaded_file = st.file_uploader("選擇Excel文件 / Excel 파일 선택", type=['xlsx'])
    
    if uploaded_file is not None:
        try:
            # Read Excel data
            df = pd.read_excel(uploaded_file)
            
            # Normalize column names
            df.columns = df.columns.str.strip()
            
            # Rename columns to match expected format if needed
            column_mapping = {
                '上傳時間': '上傳時間/업로드 시간',
                '배치번호': '批次號碼/배치 번호',
                '상점명': '店名',
                '품명': '品名',
                '색상/사이즈': '顏色/尺寸',
                '수량': '數量',
                '입고': '到貨/입고',
                '부족수량': '缺貨數量/부족 수량',
                '비고': '備註'
            }
            
            # Apply column mapping
            df.rename(columns=column_mapping, inplace=True)
            
            # Ensure required columns exist
            required_columns = [
                '批次號碼/배치 번호', 
                '上傳時間/업로드 시间', 
                '檔口', 
                '店名', 
                '品名', 
                '顏色/尺寸', 
                '數量'
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Extract floating images
            images_data = extract_floating_images(uploaded_file)
            
            # Create image HTML for each row
            image_html = {}
            for img in images_data:
                image_html[img['row_idx']] = img["image_data"]
            
            # Add images to DataFrame
            df['照片'] = df.index.map(lambda x: image_html.get(x+1, ''))
            
            # Add status columns if they don't exist
            if '到貨/입고' not in df.columns:
                df['到貨/입고'] = False
            if '缺貨數量/부족 수량' not in df.columns:
                df['缺貨數量/부족 수량'] = 0
            
            # Calculate next batch number
           
            next_batch = 1
            if not st.session_state.state["all_orders_df"].empty:
                next_batch = st.session_state.state["all_orders_df"]['批次號碼/배치 번호'].max() + 1
            
            # Add upload timestamp and batch ID
            df['上傳時間/업로드 시间'] = datetime.now()
            df['批次號碼/배치 번호'] = next_batch
            
            # Store DataFrame for preview
            st.session_state.state["latest_upload_df"] = df
            
            st.success(f'文件上傳成功！找到 {len(images_data)} 張圖片 / 파일이 성공적으로 업로드되었습니다! {len(images_data)}개의 이미지를 찾았습니다')
            
            # Preview the data
            st.subheader("預覽新訂單 / 새 주문 미리보기")
            st.data_editor(
                df,
                column_config={
                    "照片": st.column_config.ImageColumn(
                        "照片",
                        help="商品圖片 / 상품 이미지",
                        width="medium"
                    )
                },
                hide_index=True,
                disabled=True,
                height=st.session_state.viewport_height
            )
            
            # Add confirmation button
            if st.button("確認添加訂單 / 주문 추가 확인"):
                # Save to database
                save_to_db(engine, df)
                
                # Update local DataFrame
                if st.session_state.state["all_orders_df"].empty:
                    st.session_state.state["all_orders_df"] = df.copy()
                else:
                    # Reset index before concatenation
                    existing_df = st.session_state.state["all_orders_df"].reset_index(drop=True)
                    new_df = df.reset_index(drop=True)
                    
                    # Ensure both DataFrames have the same columns
                    columns_to_use = existing_df.columns
                    new_df = new_df[columns_to_use]
                    
                    # Concatenate with reset index
                    st.session_state.state["all_orders_df"] = pd.concat(
                        [existing_df, new_df], 
                        axis=0,
                        ignore_index=True
                    )
                
                st.session_state.state["latest_upload_df"] = None
                st.success("訂單已添加到系統 / 주문이 시스템에 추가되었습니다")
                st.experimental_rerun()
                
        except Exception as e:
            st.error(f'錯誤/오류: {str(e)}')

# Tab 2: Track Orders
@st.fragment
def render_tracking_tab():
    """Optimized tracking tab with minimal container creation"""
    if "tracking_tab" not in st.session_state:
        st.session_state.tracking_tab = {
            "container": st.empty(),
            "editor_key": "orders_editor"
        }
    
    with st.session_state.tracking_tab["container"]:
        st.header('追蹤訂單 / 주문 추적')
        
        if st.session_state.state["all_orders_df"].empty:
            st.info('尚無訂單資料 / 주문 데이터가 없습니다')
        else:
            orders_editor_section()

# Tab 3: Shipping Management
@st.fragment
def render_shipping_tab():
    st.header('出貨管理 / 배송 관리')
    
    if not st.session_state.state["all_orders_df"].empty:
        if st.button('新增箱子 / 새 박스 추가'):
            new_box = {
                'id': datetime.now().timestamp(),
                'items': [],
                'weight': 0,
                'shipping_fee': 0
            }
            st.session_state.state["boxes"].append(new_box)
        
        for box_idx, box in enumerate(st.session_state.state["boxes"]):
            with st.expander(f"箱子/박스 #{box_idx + 1}"):
                cols = st.columns(2)
                with cols[0]:
                    box['weight'] = st.number_input(
                        '重量/무게 (kg)', 
                        value=float(box['weight']),
                        key=f"weight_{box['id']}"
                    )
                with cols[1]:
                    box['shipping_fee'] = st.number_input(
                        '運費/운송비', 
                        value=float(box['shipping_fee']),
                        key=f"fee_{box['id']}"
                    )
                
                # Filter unassigned and unreceived orders
                mask = ~st.session_state.state["all_orders_df"]['到貨/입고']
                unassigned_orders = st.session_state.state["all_orders_df"][mask]
                
                if not unassigned_orders.empty:
                    # Create display options
                    order_options = [
                        f"{row['檔口']} - {row['店名']} - {row['品名']}"
                        for _, row in unassigned_orders.iterrows()
                    ]
                    
                    selected_order = st.selectbox(
                        '添加商品/상품 추가',
                        [''] + order_options,
                        key=f"select_{box['id']}"
                    )
                    
                    if selected_order:
                        selected_idx = order_options.index(selected_order)
                        row = unassigned_orders.iloc[selected_idx]
                        
                        # Add item to box
                        box['items'].append({
                            'stallNumber': row['檔口'],
                            'storeName': row['店名'],
                            'productName': row['品名'],
                            'specification': row['顏色/尺寸'],
                            'quantity': row['數量']
                        })
                
                if box['items']:
                    st.write('箱內商品 / 박스 내 상품:')
                    for item in box['items']:
                        st.write(f"- {item['stallNumber']} - {item['storeName']} - "
                               f"{item['productName']} ({item['specification']}) "
                               f"x{item['quantity']}")
        
        if st.session_state.state["boxes"]:
            st.subheader('出貨明細 / 배송 명세')
            total_boxes = len(st.session_state.state["boxes"])
            total_weight = sum(box['weight'] for box in st.session_state.state["boxes"])
            total_shipping = sum(box['shipping_fee'] for box in st.session_state.state["boxes"])
            
            st.write(f"總箱數/총 박스 수: {total_boxes}")
            st.write(f"總重量/총 무게: {total_weight} kg")
            st.write(f"總運費/총 운송비: ${total_shipping}")
    else:
        st.info('請先上傳訂單文件 / 주문 파일을 먼저 업로드하세요')

def main():
    st.set_page_config(page_title="Order Tracking", layout="wide")
    
    # Initialize database and tables
    engine = init_db()
    create_tables(engine)
    
    # Initialize session state
    initialize_session_state(engine)
    
    st.title('物流追蹤系統 / 물류 추적 시스템')

    tab1, tab2, tab3 = st.tabs(['上傳訂單/주문 업로드', '追蹤訂單/주문 추적', '出貨管理/배송 관리'])

    with tab1:
        render_upload_tab(engine)  # Pass engine as an argument
    with tab2:
        st.header('追蹤訂單 / 주문 추적')
        
        if st.session_state.state["all_orders_df"].empty:
            st.info('尚無訂單資料 / 주문 데이터가 없습니다')
        else:
            orders_editor_section(engine)
    with tab3:
        render_shipping_tab()

if __name__ == "__main__":
    main()