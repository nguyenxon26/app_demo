import pandas as pd
import streamlit as st

sheet_id1 = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"
gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id1}/export?format=csv&gid={gid1}"

df1 = pd.read_csv(
    url1,
    skiprows=1,
    header = 0,
    usecols= range(36)
)
df1.columns.values[1] = "Khách hàng"

# print(df1.head())

# sheet_id2 = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"
# gid2 = "782116804"
# url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id2}/export?format=csv&gid={gid2}"

# df2 = pd.read_csv(
#     url2,
#     skiprows=0,
#     header = 0,
#     usecols= range(7)
# )

# print(df2.head())

# sheet_id3 = "1v83uyh132ZbQWdgAgFUMvUdcJwoFGpoYRsr8huxWuoo"
# gid2 = "782116804"
# url2 = f"https://docs.google.com/spreadsheets/d/{sheet_id2}/export?format=csv&gid={gid2}"

# df2 = pd.read_csv(
#     url2,
#     skiprows=0,
#     header = 0,
#     usecols= range(7)
# )

# print(df2.head())

# gid3 = "1005669772"
# url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id2}/export?format=csv&gid={gid3}"

# df3 = pd.read_csv(
#     url3,
#     header = 0,
#     usecols= range(3)
# )
# print(df3)

# Chuyển các cột về dạng số, lỗi sẽ thành NaN
cols_can_chuyen = ['Tổng dư nợ tính lãi ban đầu','Tổng dư nợ đến hiện tại']
df1[cols_can_chuyen] = df1[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='raise')


df1 = df1[df1["ON/OFF Deal"] == "ON"]
debt_t = df1.groupby('Khách hàng', as_index=False)['Tổng dư nợ đến hiện tại'].sum()
debt_t = debt_t.sort_values(by ='Tổng dư nợ đến hiện tại',ascending = False).reset_index(drop=True)

# print(debt_t)

st.title('Lãi vay theo ngày')
st.dataframe(debt_t.style.format({'Tổng dư nợ đến hiện tại': '{:,.0f}'}))

