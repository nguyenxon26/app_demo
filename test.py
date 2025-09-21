# .\venv1\Scripts\activate
# pip install -r requirements.txt

import pandas as pd
import streamlit as st 
import duckdb

conn = duckdb.connect(':memory:')

sheet_id = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"

key_in = pd.read_csv(
    url1,
    skiprows=1,
    header = 0,
    usecols= range(36)
)

gid2 = "1005669772"
url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid2}"

ttkh = pd.read_csv(
    url2,
    skiprows=0,
    header = 0,
    usecols= range(3)
)

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"

NAV_batch = pd.read_csv(
    url3,
    skiprows=0,
    header = 0,
    usecols= range(7)
)

# Đổi tên cột
key_in.columns.values[1] = "khach_hang"
key_in.columns.values[6] = "ma"
key_in.columns.values[8] = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"
NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"


# Chuyển các cột về dạng số
cols_can_chuyen = ['du_no_hien_tai', 'so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_can_chuyen] = key_in[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='raise')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(
    NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False),
    errors='coerce')

# Chuyển các cột về dạng ngày tháng
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'],errors='coerce')

# Tạo bảng key in, NAV batch
conn.register('key_in',key_in)
conn.register('NAV_batch',NAV_batch)


# 1.Tạo bảng NAV real time

query1 ='''
select a.*, 
b.lai_lo_sau_cung, 
b.du_no_hien_tai,
b.gia_tri_danh_muc,
case 
    when a.NAV is null or b.gia_tri_danh_muc is null then null 
    when b.gia_tri_danh_muc = 0 then null
    else a.NAV/b.gia_tri_danh_muc 
end as ti_le
from
    (select khach_hang, sum(NAV) as NAV
        from key_in
    where khach_hang is not null
    group by khach_hang) a
left join
    (select khach_hang, 
        sum(lai_lo_sau_cung) as lai_lo_sau_cung,
        sum(du_no_hien_tai) as du_no_hien_tai,
        sum(tien_ban_phi) as gia_tri_danh_muc
        from key_in
    where on_off = 'ON'
    group by khach_hang) b
on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf()
# print(nav_daily)


# 2.Tạo bảng checkend day
query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where length(ma) = 3
and on_off = 'ON'
and khach_hang is not null
and so_luong_mua != 0
'''

checkend_day = conn.execute(query2).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot = pd.pivot_table(
    checkend_day,
    values='so_luong_mua',
    index='khach_hang',
    columns='ma',
    aggfunc='sum',
    fill_value=None,
    margins=True,
    margins_name='Tong'
)

# Sắp xếp khách hàng (rows) theo tổng mua giảm dần
pivot_no_total = pivot.drop('Tong')
sorted_rows = pivot_no_total.sort_values(by='Tong', ascending=False).index

# Sắp xếp sản phẩm (columns) theo tổng mua giảm dần
total_row = pivot.loc['Tong'].drop('Tong')
sorted_columns = total_row.sort_values(ascending=False).index
sorted_pivot = pivot.reindex(
    index=list(sorted_rows) + ['Tong'],
    columns=list(sorted_columns) 
)

# 3.Tạo bảng lãi vay theo ngày
query3 = '''
select khach_hang, ngay, lai_vay_ngay
from NAV_batch
'''

lai_ngay = conn.execute(query3).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot_2 = pd.pivot_table(
    NAV_batch,
    values='lai_vay_ngay',
    index='khach_hang',
    columns='ngay',
    aggfunc='sum',
    fill_value=None,
    # margins=True,
    # margins_name='Tong'
)
pivot_2 = pivot_2.sort_index(axis=1, ascending=True)

pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False)
pivot_2 = pivot_2.drop(columns='__tong_tam__')
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])
pivot_2.columns = [col.strftime('%d/%m/%Y') for col in pivot_2.columns]
print(pivot_2)


# Streamlit UI

st.title('🧮 Dashboard Khách hàng')
st.header('📈 NAV ngày')
st.dataframe(nav_daily.style.format({
    'NAV':'{:,.0f}',
    'lai_lo_sau_cung':'{:,.0f}', 
    'du_no_hien_tai':'{:,.0f}',
    'gia_tri_danh_muc':'{:,.0f}',
    'ti_le': '{:.2%}'})
        .apply(lambda x: ['background-color: lightgreen' if v == x.max() else '' for v in x], 
               subset=[col for col in nav_daily.columns if col != 'khach_hang'])
        )


st.header('🛒 Số lượng mua ')
st.dataframe(sorted_pivot.style.format('{:,.0f}'))


# khach_hang_list = NAV_batch['khach_hang'].unique()
# selected_khach = st.selectbox('Chọn khách hàng', khach_hang_list)
# filtered_data = NAV_batch[NAV_batch['khach_hang'] == selected_khach]

st.header('💰 Lãi vay theo ngày')
st.table(pivot_2.style.format('{:,.0f}')
            .highlight_max(axis=1, color='lightgreen')
            # .highlight_min(axis=1, color='lightcoral')
            )
st.subheader("📊 Tổng lãi vay theo ngày")
st.line_chart(pivot_2.sum(axis=0))
# .\venv1\Scripts\activate
# pip install -r requirements.txt

import pandas as pd
import streamlit as st 
import duckdb

conn = duckdb.connect(':memory:')

sheet_id = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"

key_in = pd.read_csv(
    url1,
    skiprows=1,
    header = 0,
    usecols= range(36)
)

gid2 = "1005669772"
url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid2}"

ttkh = pd.read_csv(
    url2,
    skiprows=0,
    header = 0,
    usecols= range(3)
)

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"

NAV_batch = pd.read_csv(
    url3,
    skiprows=0,
    header = 0,
    usecols= range(7)
)

# Đổi tên cột
key_in.columns.values[1] = "khach_hang"
key_in.columns.values[6] = "ma"
key_in.columns.values[8] = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"
NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"


# Chuyển các cột về dạng số
cols_can_chuyen = ['du_no_hien_tai', 'so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_can_chuyen] = key_in[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='raise')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(
    NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False),
    errors='coerce')

# Chuyển các cột về dạng ngày tháng
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'],errors='coerce')

# Tạo bảng key in, NAV batch
conn.register('key_in',key_in)
conn.register('NAV_batch',NAV_batch)


# 1.Tạo bảng NAV real time

query1 ='''
select a.*, 
b.lai_lo_sau_cung, 
b.du_no_hien_tai,
b.gia_tri_danh_muc,
case 
    when a.NAV is null or b.gia_tri_danh_muc is null then null 
    when b.gia_tri_danh_muc = 0 then null
    else a.NAV/b.gia_tri_danh_muc 
end as ti_le
from
    (select khach_hang, sum(NAV) as NAV
        from key_in
    where khach_hang is not null
    group by khach_hang) a
left join
    (select khach_hang, 
        sum(lai_lo_sau_cung) as lai_lo_sau_cung,
        sum(du_no_hien_tai) as du_no_hien_tai,
        sum(tien_ban_phi) as gia_tri_danh_muc
        from key_in
    where on_off = 'ON'
    group by khach_hang) b
on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf()
# print(nav_daily)


# 2.Tạo bảng checkend day
query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where length(ma) = 3
and on_off = 'ON'
and khach_hang is not null
and so_luong_mua != 0
'''

checkend_day = conn.execute(query2).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot = pd.pivot_table(
    checkend_day,
    values='so_luong_mua',
    index='khach_hang',
    columns='ma',
    aggfunc='sum',
    fill_value=None,
    margins=True,
    margins_name='Tong'
)

# Sắp xếp khách hàng (rows) theo tổng mua giảm dần
pivot_no_total = pivot.drop('Tong')
sorted_rows = pivot_no_total.sort_values(by='Tong', ascending=False).index

# Sắp xếp sản phẩm (columns) theo tổng mua giảm dần
total_row = pivot.loc['Tong'].drop('Tong')
sorted_columns = total_row.sort_values(ascending=False).index
sorted_pivot = pivot.reindex(
    index=list(sorted_rows) + ['Tong'],
    columns=list(sorted_columns) 
)

# 3.Tạo bảng lãi vay theo ngày
query3 = '''
select khach_hang, ngay, lai_vay_ngay
from NAV_batch
'''

lai_ngay = conn.execute(query3).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot_2 = pd.pivot_table(
    NAV_batch,
    values='lai_vay_ngay',
    index='khach_hang',
    columns='ngay',
    aggfunc='sum',
    fill_value=None,
    # margins=True,
    # margins_name='Tong'
)
pivot_2 = pivot_2.sort_index(axis=1, ascending=True)

pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False)
pivot_2 = pivot_2.drop(columns='__tong_tam__')
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])
pivot_2.columns = [col.strftime('%d/%m/%Y') for col in pivot_2.columns]
print(pivot_2)


# Streamlit UI

st.title('🧮 Dashboard Khách hàng')
st.header('📈 NAV ngày')
st.dataframe(nav_daily.style.format({
    'NAV':'{:,.0f}',
    'lai_lo_sau_cung':'{:,.0f}', 
    'du_no_hien_tai':'{:,.0f}',
    'gia_tri_danh_muc':'{:,.0f}',
    'ti_le': '{:.2%}'})
        .apply(lambda x: ['background-color: lightgreen' if v == x.max() else '' for v in x], 
               subset=[col for col in nav_daily.columns if col != 'khach_hang'])
        )


st.header('🛒 Số lượng mua ')
st.dataframe(sorted_pivot.style.format('{:,.0f}'))


# khach_hang_list = NAV_batch['khach_hang'].unique()
# selected_khach = st.selectbox('Chọn khách hàng', khach_hang_list)
# filtered_data = NAV_batch[NAV_batch['khach_hang'] == selected_khach]

st.header('💰 Lãi vay theo ngày')
st.table(pivot_2.style.format('{:,.0f}')
            .highlight_max(axis=1, color='lightgreen')
            # .highlight_min(axis=1, color='lightcoral')
            )
st.subheader("📊 Tổng lãi vay theo ngày")
st.line_chart(pivot_2.sum(axis=0))
# .\venv1\Scripts\activate
# pip install -r requirements.txt

import pandas as pd
import streamlit as st 
import duckdb

conn = duckdb.connect(':memory:')

sheet_id = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"

key_in = pd.read_csv(
    url1,
    skiprows=1,
    header = 0,
    usecols= range(36)
)

gid2 = "1005669772"
url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid2}"

ttkh = pd.read_csv(
    url2,
    skiprows=0,
    header = 0,
    usecols= range(3)
)

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"

NAV_batch = pd.read_csv(
    url3,
    skiprows=0,
    header = 0,
    usecols= range(7)
)

# Đổi tên cột
key_in.columns.values[1] = "khach_hang"
key_in.columns.values[6] = "ma"
key_in.columns.values[8] = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"
NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"


# Chuyển các cột về dạng số
cols_can_chuyen = ['du_no_hien_tai', 'so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_can_chuyen] = key_in[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='raise')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(
    NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False),
    errors='coerce')

# Chuyển các cột về dạng ngày tháng
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'],errors='coerce')

# Tạo bảng key in, NAV batch
conn.register('key_in',key_in)
conn.register('NAV_batch',NAV_batch)


# 1.Tạo bảng NAV real time

query1 ='''
select a.*, 
b.lai_lo_sau_cung, 
b.du_no_hien_tai,
b.gia_tri_danh_muc,
case 
    when a.NAV is null or b.gia_tri_danh_muc is null then null 
    when b.gia_tri_danh_muc = 0 then null
    else a.NAV/b.gia_tri_danh_muc 
end as ti_le
from
    (select khach_hang, sum(NAV) as NAV
        from key_in
    where khach_hang is not null
    group by khach_hang) a
left join
    (select khach_hang, 
        sum(lai_lo_sau_cung) as lai_lo_sau_cung,
        sum(du_no_hien_tai) as du_no_hien_tai,
        sum(tien_ban_phi) as gia_tri_danh_muc
        from key_in
    where on_off = 'ON'
    group by khach_hang) b
on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf()
# print(nav_daily)


# 2.Tạo bảng checkend day
query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where length(ma) = 3
and on_off = 'ON'
and khach_hang is not null
and so_luong_mua != 0
'''

checkend_day = conn.execute(query2).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot = pd.pivot_table(
    checkend_day,
    values='so_luong_mua',
    index='khach_hang',
    columns='ma',
    aggfunc='sum',
    fill_value=None,
    margins=True,
    margins_name='Tong'
)

# Sắp xếp khách hàng (rows) theo tổng mua giảm dần
pivot_no_total = pivot.drop('Tong')
sorted_rows = pivot_no_total.sort_values(by='Tong', ascending=False).index

# Sắp xếp sản phẩm (columns) theo tổng mua giảm dần
total_row = pivot.loc['Tong'].drop('Tong')
sorted_columns = total_row.sort_values(ascending=False).index
sorted_pivot = pivot.reindex(
    index=list(sorted_rows) + ['Tong'],
    columns=list(sorted_columns) 
)

# 3.Tạo bảng lãi vay theo ngày
query3 = '''
select khach_hang, ngay, lai_vay_ngay
from NAV_batch
'''

lai_ngay = conn.execute(query3).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot_2 = pd.pivot_table(
    NAV_batch,
    values='lai_vay_ngay',
    index='khach_hang',
    columns='ngay',
    aggfunc='sum',
    fill_value=None,
    # margins=True,
    # margins_name='Tong'
)
pivot_2 = pivot_2.sort_index(axis=1, ascending=True)

pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False)
pivot_2 = pivot_2.drop(columns='__tong_tam__')
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])
pivot_2.columns = [col.strftime('%d/%m/%Y') for col in pivot_2.columns]
print(pivot_2)


# Streamlit UI

st.title('🧮 Dashboard Khách hàng')
st.header('📈 NAV ngày')
st.dataframe(nav_daily.style.format({
    'NAV':'{:,.0f}',
    'lai_lo_sau_cung':'{:,.0f}', 
    'du_no_hien_tai':'{:,.0f}',
    'gia_tri_danh_muc':'{:,.0f}',
    'ti_le': '{:.2%}'})
        .apply(lambda x: ['background-color: lightgreen' if v == x.max() else '' for v in x], 
               subset=[col for col in nav_daily.columns if col != 'khach_hang'])
        )


st.header('🛒 Số lượng mua ')
st.dataframe(sorted_pivot.style.format('{:,.0f}'))


# khach_hang_list = NAV_batch['khach_hang'].unique()
# selected_khach = st.selectbox('Chọn khách hàng', khach_hang_list)
# filtered_data = NAV_batch[NAV_batch['khach_hang'] == selected_khach]

st.header('💰 Lãi vay theo ngày')
st.table(pivot_2.style.format('{:,.0f}')
            .highlight_max(axis=1, color='lightgreen')
            # .highlight_min(axis=1, color='lightcoral')
            )
st.subheader("📊 Tổng lãi vay theo ngày")
st.line_chart(pivot_2.sum(axis=0))
# .\venv1\Scripts\activate
# pip install -r requirements.txt

import pandas as pd
import streamlit as st 
import duckdb

conn = duckdb.connect(':memory:')

sheet_id = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"

key_in = pd.read_csv(
    url1,
    skiprows=1,
    header = 0,
    usecols= range(36)
)

gid2 = "1005669772"
url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid2}"

ttkh = pd.read_csv(
    url2,
    skiprows=0,
    header = 0,
    usecols= range(3)
)

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"

NAV_batch = pd.read_csv(
    url3,
    skiprows=0,
    header = 0,
    usecols= range(7)
)

# Đổi tên cột
key_in.columns.values[1] = "khach_hang"
key_in.columns.values[6] = "ma"
key_in.columns.values[8] = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"
NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"


# Chuyển các cột về dạng số
cols_can_chuyen = ['du_no_hien_tai', 'so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_can_chuyen] = key_in[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='raise')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(
    NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False),
    errors='coerce')

# Chuyển các cột về dạng ngày tháng
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'],errors='coerce')

# Tạo bảng key in, NAV batch
conn.register('key_in',key_in)
conn.register('NAV_batch',NAV_batch)


# 1.Tạo bảng NAV real time

query1 ='''
select a.*, 
b.lai_lo_sau_cung, 
b.du_no_hien_tai,
b.gia_tri_danh_muc,
case 
    when a.NAV is null or b.gia_tri_danh_muc is null then null 
    when b.gia_tri_danh_muc = 0 then null
    else a.NAV/b.gia_tri_danh_muc 
end as ti_le
from
    (select khach_hang, sum(NAV) as NAV
        from key_in
    where khach_hang is not null
    group by khach_hang) a
left join
    (select khach_hang, 
        sum(lai_lo_sau_cung) as lai_lo_sau_cung,
        sum(du_no_hien_tai) as du_no_hien_tai,
        sum(tien_ban_phi) as gia_tri_danh_muc
        from key_in
    where on_off = 'ON'
    group by khach_hang) b
on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf()
# print(nav_daily)


# 2.Tạo bảng checkend day
query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where length(ma) = 3
and on_off = 'ON'
and khach_hang is not null
and so_luong_mua != 0
'''

checkend_day = conn.execute(query2).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot = pd.pivot_table(
    checkend_day,
    values='so_luong_mua',
    index='khach_hang',
    columns='ma',
    aggfunc='sum',
    fill_value=None,
    margins=True,
    margins_name='Tong'
)

# Sắp xếp khách hàng (rows) theo tổng mua giảm dần
pivot_no_total = pivot.drop('Tong')
sorted_rows = pivot_no_total.sort_values(by='Tong', ascending=False).index

# Sắp xếp sản phẩm (columns) theo tổng mua giảm dần
total_row = pivot.loc['Tong'].drop('Tong')
sorted_columns = total_row.sort_values(ascending=False).index
sorted_pivot = pivot.reindex(
    index=list(sorted_rows) + ['Tong'],
    columns=list(sorted_columns) 
)

# 3.Tạo bảng lãi vay theo ngày
query3 = '''
select khach_hang, ngay, lai_vay_ngay
from NAV_batch
'''

lai_ngay = conn.execute(query3).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot_2 = pd.pivot_table(
    NAV_batch,
    values='lai_vay_ngay',
    index='khach_hang',
    columns='ngay',
    aggfunc='sum',
    fill_value=None,
    # margins=True,
    # margins_name='Tong'
)
pivot_2 = pivot_2.sort_index(axis=1, ascending=True)

pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False)
pivot_2 = pivot_2.drop(columns='__tong_tam__')
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])
pivot_2.columns = [col.strftime('%d/%m/%Y') for col in pivot_2.columns]
print(pivot_2)


# Streamlit UI

st.title('🧮 Dashboard Khách hàng')
st.header('📈 NAV ngày')
st.dataframe(nav_daily.style.format({
    'NAV':'{:,.0f}',
    'lai_lo_sau_cung':'{:,.0f}', 
    'du_no_hien_tai':'{:,.0f}',
    'gia_tri_danh_muc':'{:,.0f}',
    'ti_le': '{:.2%}'})
        .apply(lambda x: ['background-color: lightgreen' if v == x.max() else '' for v in x], 
               subset=[col for col in nav_daily.columns if col != 'khach_hang'])
        )


st.header('🛒 Số lượng mua ')
st.dataframe(sorted_pivot.style.format('{:,.0f}'))


# khach_hang_list = NAV_batch['khach_hang'].unique()
# selected_khach = st.selectbox('Chọn khách hàng', khach_hang_list)
# filtered_data = NAV_batch[NAV_batch['khach_hang'] == selected_khach]

st.header('💰 Lãi vay theo ngày')
st.dataframe(pivot_2.style.format('{:,.0f}')
            .highlight_max(axis=1, color='lightgreen')
            # .highlight_min(axis=1, color='lightcoral')
            )
st.subheader("📊 Tổng lãi vay theo ngày")
st.line_chart(pivot_2.sum(axis=0))

