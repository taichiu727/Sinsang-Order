import streamlit as st
import pandas as pd
from datetime import datetime
import os
import io
from PIL import Image
import openpyxl
import tempfile
import numpy as np
import base64
from sqlalchemy import create_engine, text
import json
import time

# Database configuration
DATABASE_URL = "postgresql://neondb_owner:npg_WEzy0a1pQMAl@ep-delicate-forest-a1jhkrwa-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
DEBOUNCE_INTERVAL = 2
# Initialize database connection
@st.cache_resource
def init_db():
    return create_engine(DATABASE_URL)

def extract_floating_images(excel_file):
    """
    Extract floating images from Excel file and match them to rows
    """
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

# Database operations
def create_tables(engine):
    with engine.connect() as conn:
        # Create orders table
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
    with engine.connect() as conn:
        # Convert DataFrame rows to database records
        for _, row in df.iterrows():
            # Convert DataFrame to dict and handle datetime serialization
            row_dict = row.copy()
            if isinstance(row_dict.get('上傳時間/업로드 시간'), pd.Timestamp):
                row_dict['上傳時間/업로드 시간'] = row_dict['上傳時間/업로드 시간'].strftime('%Y-%m-%d %H:%M:%S')
            original_data = row_dict.to_dict()
            
            # Insert into database
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
                    "upload_time": pd.to_datetime(row.get('上傳時間/업로드 시간')),
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
        # Read all orders from database
        result = conn.execute(text("""
            SELECT 
                batch_number as "批次號碼/배치 번호",
                upload_time as "上傳時間/업로드 시간",
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
        
        # Convert to DataFrame
        df = pd.DataFrame(result.fetchall())
        
        if not df.empty:
            # Convert upload_time to datetime
            df['上傳時間/업로드 시간'] = pd.to_datetime(df['上傳時間/업로드 시간'])
            
            # Ensure consistent column order
            column_order = [
                '批次號碼/배치 번호',
                '上傳時間/업로드 시간',
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
            
            # Reorder columns
            df = df[column_order]
            
        return df

@st.fragment
def batch_update_db(engine, changes_dict, base_df):
    """
    Perform batch updates to the database efficiently
    """
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # Prepare batch update statement
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
                
                # Process all changes in a single transaction
                for row_idx, changes in changes_dict.items():
                    row_idx = int(row_idx)
                    row = base_df.iloc[row_idx]
                    
                    # Prepare update parameters
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

# Set page to wide mode
st.set_page_config(layout="wide")

# Initialize database
engine = init_db()
create_tables(engine)


def debounced_save(_changes, _df):
    """
    Debounced save function to prevent rapid consecutive saves
    """
    return batch_update_db(engine, _changes, _df)

@st.fragment
def initialize_session_state():
   
    if "viewport_height" not in st.session_state:
        st.session_state.viewport_height = 800
    if "state" not in st.session_state:
        st.session_state.state = {
            "all_orders_df": load_from_db(engine),
            "latest_upload_df": None,
            "boxes": [],
            "edited_df": None,
            "pending_changes": False
        }
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = time.time()
    if 'pending_changes' not in st.session_state:
        st.session_state.pending_changes = {}
    if 'save_status' not in st.session_state:
        st.session_state.save_status = None
    # Ensure proper data types for the initial DataFrame
    if not st.session_state.state["all_orders_df"].empty:
        st.session_state.state["all_orders_df"]["到貨/입고"] = st.session_state.state["all_orders_df"]["到貨/입고"].astype(bool)
        st.session_state.state["all_orders_df"]["缺貨數量/부족 수량"] = st.session_state.state["all_orders_df"]["缺貨數量/부족 수량"].fillna(0).astype(int)

def process_changes():
    """Process pending changes if debounce interval has passed"""
    current_time = time.time()
    if (current_time - st.session_state.last_save_time >= DEBOUNCE_INTERVAL and 
        st.session_state.pending_changes):
        
        if batch_update_db(engine, st.session_state.pending_changes, st.session_state.state["edited_df"]):
            # Update the DataFrame with confirmed changes
            current_df = st.session_state.state["edited_df"].copy()
            for row_idx, changes in st.session_state.pending_changes.items():
                row_idx = int(row_idx)
                for column, new_value in changes.items():
                    current_df.at[row_idx, column] = new_value
            
            st.session_state.state["all_orders_df"] = current_df.copy()
            st.session_state.state["edited_df"] = current_df
            st.session_state.pending_changes = {}
            st.session_state.save_status = "success"
        else:
            st.session_state.save_status = "error"
        
        st.session_state.last_save_time = current_time

@st.fragment
def orders_editor_section():
    """Optimized orders editor section with minimal rerenders"""
    # Initialize containers using st.empty() for granular updates
    if "editor_containers" not in st.session_state:
        st.session_state.editor_containers = {
            "stats": st.empty(),
            "editor": st.empty(),
            "status": st.empty()
        }
    
    # Initialize edited_df if needed
    if st.session_state.state["edited_df"] is None:
        st.session_state.state["edited_df"] = st.session_state.state["all_orders_df"].copy()

    # Process any pending changes without triggering rerender
    current_time = time.time()
    if (current_time - st.session_state.last_save_time >= DEBOUNCE_INTERVAL and 
        st.session_state.pending_changes):
        
        if batch_update_db(engine, st.session_state.pending_changes, st.session_state.state["edited_df"]):
            current_df = st.session_state.state["edited_df"].copy()
            st.session_state.state["all_orders_df"] = current_df.copy()
            st.session_state.pending_changes = {}
            with st.session_state.editor_containers["status"]:
                st.toast("✅ Changes saved", icon="✅")
        else:
            with st.session_state.editor_containers["status"]:
                st.error("Failed to save changes")
        
        st.session_state.last_save_time = current_time

    # Update statistics only when needed
    with st.session_state.editor_containers["stats"]:
        display_order_statistics(st.session_state.state["edited_df"])

    # Render the data editor with optimized configuration
    with st.session_state.editor_containers["editor"]:
        return st.data_editor(
            st.session_state.state["edited_df"],
            column_config={
                "照片": st.column_config.ImageColumn("照片", help="商品圖片 / 상품 이미지", width="medium"),
                "到貨/입고": st.column_config.CheckboxColumn("到貨/입고", help="勾選表示已收到 / 수령 확인", default=False),
                "缺貨數量/부족 수량": st.column_config.NumberColumn("缺貨數量/부족 수량", help="輸入缺貨數量 / 부족 수량 입력", min_value=0, step=1),
                "備註": st.column_config.TextColumn("備註", help="點擊編輯備註 / 비고 편집", width="large"),
                "上傳時間/업로드 시간": st.column_config.DatetimeColumn("上傳時間/업로드 시간", help="訂單上傳時間 / 주문 업로드 시간", width="medium", format="YYYY-MM-DD HH:mm:ss"),
                "批次號碼/배치 번호": st.column_config.NumberColumn("批次號碼/배치 번호", help="訂單批次 / 주문 배치", width="small"),
            },
            hide_index=True,
            height=st.session_state.viewport_height,
            disabled=["批次號碼/배치 번호", "上傳時間/업로드 시간", "照片", "檔口", "店名", "品名", "顏色/尺寸", "數量"],
            on_change=handle_edit,
            key=st.session_state.tracking_tab["editor_key"],
            use_container_width=True
        )

@st.fragment
def handle_edit():
    """Optimized edit handler with minimal state updates"""
    edited_data = st.session_state.orders_editor
    
    if edited_data is not None and "edited_rows" in edited_data and edited_data["edited_rows"]:
        changes = edited_data["edited_rows"]
        current_df = st.session_state.state["edited_df"]
        
        # Update DataFrame and pending changes in a single pass
        for row_idx, row_changes in changes.items():
            row_idx = int(row_idx)
            if row_idx not in st.session_state.pending_changes:
                st.session_state.pending_changes[row_idx] = {}
            
            for column, new_value in row_changes.items():
                if column == "到貨/입고":
                    value = bool(new_value)
                    current_df.at[row_idx, column] = value
                    st.session_state.pending_changes[row_idx][column] = value
                elif column == "缺貨數量/부족 수량":
                    value = int(new_value) if pd.notna(new_value) else 0
                    current_df.at[row_idx, column] = value
                    st.session_state.pending_changes[row_idx][column] = value
                elif column == "備註":
                    value = str(new_value) if pd.notna(new_value) else ''
                    current_df.at[row_idx, column] = value
                    st.session_state.pending_changes[row_idx][column] = value

@st.fragment
def save_changes():
    """Save pending changes to database"""
    if st.session_state.state["pending_changes"]:
        try:
            if st.session_state.state["edited_df"] is None:
                st.error("No changes to save")
                return

            with st.spinner('Saving changes...'):
                if update_db(engine, st.session_state.state["edited_df"]):
                    fresh_data = load_from_db(engine)
                    st.session_state.state["all_orders_df"] = fresh_data
                    st.session_state.state["edited_df"] = fresh_data.copy()
                    st.session_state.state["pending_changes"] = False
                    st.toast("✅ Changes saved successfully")
                else:
                    st.error("Failed to save changes")
                    st.session_state.state["pending_changes"] = True
        except Exception as e:
            st.error(f"Error saving changes: {str(e)}")
            st.session_state.state["pending_changes"] = True
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
def render_upload_tab():
    st.header('上傳訂單 / 주문 업로드')
    uploaded_file = st.file_uploader("選擇Excel文件 / Excel 파일 선택", type=['xlsx'])
    
    if uploaded_file is not None:
        try:
            # Read Excel data
            df = pd.read_excel(uploaded_file)
            
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
            df['上傳時間/업로드 시간'] = datetime.now()
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
                st.rerun()
                
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

# Main app
st.title('物流追蹤系統 / 물류 추적 시스템')

tab1, tab2, tab3 = st.tabs(['上傳訂單/주문 업로드', '追蹤訂單/주문 추적', '出貨管理/배송 관리'])

initialize_session_state()

with tab1:
    render_upload_tab()
with tab2:
    render_tracking_tab()
with tab3:
    render_shipping_tab()