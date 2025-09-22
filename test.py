import pandas as pd
import streamlit as st 
import duckdb

st.set_page_config(layout="wide")

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

# gid2 = "1005669772"
# url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid2}"

# ttkh = pd.read_csv(
#     url2,
#     skiprows=0,
#     header = 0,
#     usecols= range(3)
# )

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
select khach_hang,
    ngay,
    lai_vay_ngay
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
# 1. Sắp xếp lại cột theo thời gian tăng dần
pivot_2 = pivot_2.sort_index(axis=1)

# 2. Sắp xếp theo tổng hàng
pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False).drop(columns='__tong_tam__')

# 3. Thêm dòng tổng
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])

# 4. Chuyển cột về datetime nếu chưa
pivot_2.columns = pd.to_datetime(pivot_2.columns)

# 5. Tính thay đổi tuyệt đối
pivot_2_no_total = pivot_2.drop(index='Tổng')
pivot_2_diff = pivot_2_no_total.diff(axis=1)
pivot_2_diff = pd.concat([pivot_2_diff, pd.DataFrame(index=['Tổng'], columns=pivot_2_diff.columns)])

# 6. Tạo cột xen kẽ: giá trị + thay đổi tuyệt đối
merged_cols = []
for col in pivot_2.columns:
    col_str = col.strftime('%d/%m/%Y')
    merged_cols.append(col_str)
    if col != pivot_2.columns[0]:
        merged_cols.append(f'{col_str} (thay đổi)')

# 7. Tạo DataFrame kết hợp
pivot_2_combined = pd.DataFrame(index=pivot_2.index, columns=merged_cols)

for col in pivot_2.columns:
    col_str = col.strftime('%d/%m/%Y')
    pivot_2_combined[col_str] = pivot_2[col]
    if col != pivot_2.columns[0]:
        diff_series = pivot_2_diff[col].apply(lambda x: f"+{x:,.0f}" if x > 0 else (f"{x:,.0f}" if x < 0 else ""))
        pivot_2_combined[f'{col_str} (thay đổi)'] = diff_series

# 8. Đảo ngược thứ tự ngày
sorted_dates = sorted(pivot_2.columns, reverse=True)
final_col_order = []
for col in sorted_dates:
    col_str = col.strftime('%d/%m/%Y')
    final_col_order.append(col_str)
    diff_col = f'{col_str} (thay đổi)'
    if diff_col in pivot_2_combined.columns:
        final_col_order.append(diff_col)

pivot_2_combined = pivot_2_combined[final_col_order]


# Tạo bảng tổng lãi vay theo ngày
query4 = '''
select ngay, 
sum(lai_vay_ngay) as lai_vay_tong
from NAV_batch
group by ngay
'''
lai_tong = conn.execute(query4).fetchdf()
lai_tong.set_index('ngay', inplace=True)



# Streamlit UI
col1, col2 = st.columns(2)


nav_daily_renamed = nav_daily.rename(columns={
    'khach_hang' : 'Khách hàng',
    'lai_lo_sau_cung': 'Lãi lỗ sau cùng',
    'du_no_hien_tai': 'Dư nợ hiện tại',
    'gia_tri_danh_muc': 'Giá trị danh mục',
    'ti_le': 'Tỉ lệ'
})

nav_daily_renamed = nav_daily_renamed.replace([None, pd.NA, "None"], "")  # Handle None and pd.NA
nav_daily_renamed = nav_daily_renamed.fillna("")  # Handle NaN
nav_daily_renamed.replace(0, "", inplace=True)  # Handle zeros

# Convert specified columns to numeric, coercing errors to NaN
numeric_columns = ['NAV', 'Lãi lỗ sau cùng', 'Dư nợ hiện tại', 'Giá trị danh mục', 'Tỉ lệ']
for col in numeric_columns:
    nav_daily_renamed[col] = pd.to_numeric(nav_daily_renamed[col], errors='coerce')

# print(nav_daily_renamed.isna().sum())
print(nav_daily_renamed.apply(lambda x: x.isin([None]).sum()))


st.title('🧮 Dashboard Khách hàng')

# Tạo 2 cột
col1, col2 = st.columns(2)

with col1:
    st.header('📈 NAV ngày')
    st.dataframe(nav_daily_renamed.style.format({
        'NAV': '{:,.0f}',
        'Lãi lỗ sau cùng': '{:,.0f}',
        'Dư nợ hiện tại': '{:,.0f}',
        'Giá trị danh mục': '{:,.0f}',
        'Tỉ lệ': '{:.2%}'
    }, na_rep="")
        .apply(lambda x: ['background-color: lightgreen' if v == x.max() else '' for v in x],
               subset=[col for col in nav_daily_renamed.columns if col != 'Khách hàng']),
        height=600
    )

    st.markdown("<br>", unsafe_allow_html=True)  # Khoảng cách giữa 2 bảng

    st.header('💰 Lãi vay theo ngày')
    fmt_dict = {}
    for col in pivot_2_combined.columns:
        if '(thay đổi)' in col:
            continue
        fmt_dict[col] = lambda x: '' if pd.isna(x) or x == 0 else f'{x:,.0f}'

    def highlight(val):
        if isinstance(val, str):
            val = val.replace(",", "").replace("+", "").strip()
            try:
                num = float(val)
                if num > 0:
                    return 'color: green'
                elif num < 0:
                    return 'color: red'
            except:
                pass
        return ''

    thay_doi_cols = [col for col in pivot_2_combined.columns if '(thay đổi)' in col]

    st.dataframe(
        pivot_2_combined.style
            .format(fmt_dict)
            .applymap(highlight, subset=thay_doi_cols),
        height=600
    )

with col2:
    st.header('🛒 Số lượng mua')
    st.dataframe(
        sorted_pivot.style.format(lambda x: "" if pd.isna(x) or x == 0 else f"{x:,.0f}"),
        height=600
    )

    st.markdown("<br>", unsafe_allow_html=True)  # Khoảng cách

    st.subheader("📊 Tổng lãi vay theo ngày")
    st.line_chart(lai_tong['lai_vay_tong'])


