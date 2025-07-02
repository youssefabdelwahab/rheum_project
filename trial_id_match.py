import pandas as pd

df1 = pd.read_csv("updated_paper_info.csv")
df2 = pd.read_excel("Living database of RA trials_Latest version to share_withCRSID_2025.xlsx")

filtered_df1 = df1[df1["type"] == "PDF"]

df1_selected = filtered_df1[["id","pdf_url", "type"]]
df2_selected = df2[["recordid.", "clinical_reg_no"]]

merged_df = pd.merge(
    df1_selected,
    df2_selected,
    left_on = "id",
    right_on = "recordid.",
    how = "left"
)

merged_df.to_csv("output_for_pdf.csv", index = False)



