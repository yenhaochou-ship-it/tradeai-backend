"""
analyze_shap.py — 5天驗證期跑完後，用真實交易資料做特徵歸因分析(SHAP)

═══════════════════════════════════════════════════════════════════════════
用法
═══════════════════════════════════════════════════════════════════════════
1. pip install shap pandas numpy lightgbm matplotlib --break-system-packages

2. 從你的後端下載特徵紀錄CSV（這是「實際發生過的交易」，不是「每次掃描評估過的候選」，
   因為要分析「特徵跟真實盈虧的關係」，候選沒成交就沒有盈虧可以對照）：
   curl -o paper_test_data.csv https://你的後端網址/auto/feature_log.csv
   （或直接瀏覽器開啟這個網址，會自動下載）

3. 確保 lgbm_model.txt 在這個腳本旁邊（跟訓練時產生的同一個檔案）

4. python analyze_shap.py

═══════════════════════════════════════════════════════════════════════════
跟原始文件規格的差異
═══════════════════════════════════════════════════════════════════════════
原始文件假設有個 paper_test_data.csv 是「你自己手動存下來的」——但檢查過main.py後發現，
原本的系統從來沒有把預測時用的特徵向量存下來，算完就丟，所以那份CSV實際上不存在，
分析計畫做不了。已經在main.py加了完整的記錄管線(_record_feature_log)，這個腳本讀的
就是那個真正會被產生出來的CSV，欄位名稱跟main.py的ML_FEATURE_NAMES逐項對應，不是猜的。

只記錄「實際成交的交易」(20筆驗證門檻那些)，不是每30秒對股票池每檔股票都記一筆——
後者資料量會大得多，但大部分都是「沒有成交、沒有結果可以對照」的候選，對「特徵跟盈虧
關係」這個分析目的來說，反而是雜訊不是訊號。如果你之後想做更全面的「候選 vs 沒成交」
分析，要另外加一個基於候選層級而非交易層級的log，這個腳本目前沒有處理那種情境。
"""
import argparse
import pandas as pd
import numpy as np

ML_FEATURE_NAMES=[
    "rsi","williams_r","cci","trend_str","vol_ratio",
    "ma5_ma20_dev_pct","price_vwap_dev_pct",
    "regime_code","mtf_aligned_code",
    "volume_quality_score","bid_ask_imbalance","order_flow_delta_scaled","orb_breakout",
    "real_ofi_interval","real_big_trade_interval_scaled",
]

def main():
    ap=argparse.ArgumentParser(description="LightGBM特徵歸因分析(SHAP)")
    ap.add_argument("--csv",default="paper_test_data.csv",help="從/auto/feature_log.csv下載的特徵紀錄")
    ap.add_argument("--model",default="lgbm_model.txt",help="訓練好的LightGBM模型檔案")
    ap.add_argument("--no-plot",action="store_true",help="不開圖形視窗(例如在沒有顯示器的伺服器上跑)，只印文字結果")
    args=ap.parse_args()

    df=pd.read_csv(args.csv)
    print(f"讀到{len(df)}筆實際交易紀錄")
    if len(df)<20:
        print(f"⚠️ 只有{len(df)}筆，少於建議的20筆驗證門檻——分析結果僅供參考，樣本太少容易看到雜訊而不是真訊號。")

    missing=[c for c in ML_FEATURE_NAMES if c not in df.columns]
    if missing:
        print(f"❌ CSV缺少這些特徵欄位: {missing}")
        print("   可能main.py的ML_FEATURE_NAMES跟這個腳本不一致了，檢查兩邊是否同步更新過。")
        return
    X=df[ML_FEATURE_NAMES]

    import lightgbm as lgb
    model=lgb.Booster(model_file=args.model)

    # ── 步驟一：SHAP值，看每個特徵把預測機率往哪個方向推、推多大力 ──
    import shap
    explainer=shap.TreeExplainer(model)
    shap_values=explainer.shap_values(X)
    # shap套件不同版本對二元分類的回傳格式不一致：有時是單一2D array，有時是[class0_array, class1_array]，
    # 用list長度判斷比假設固定格式安全，不然換一個shap版本這支腳本可能直接壞掉或算出錯誤結果。
    if isinstance(shap_values,list):
        shap_values=shap_values[1] if len(shap_values)>1 else shap_values[0]

    print("\n=== 步驟一：特徵重要性排名(依SHAP值絕對值平均，越大代表對預測的影響力越大) ===")
    importance=pd.DataFrame({
        "feature":ML_FEATURE_NAMES,
        "mean_abs_shap":np.abs(shap_values).mean(axis=0),
        "mean_shap":shap_values.mean(axis=0),  # 正值=平均把機率往上推(偏多)，負值=往下壓(偏空)
    }).sort_values("mean_abs_shap",ascending=False)
    print(importance.to_string(index=False))
    near_zero=importance[importance["mean_abs_shap"]<importance["mean_abs_shap"].max()*0.05]["feature"].tolist()
    if near_zero:
        print(f"\n💡 這些特徵的SHAP值接近0，這5天的實戰資料顯示它們幾乎沒有預測力，是候選的剔除對象：{near_zero}")
        print("   注意：5天/20筆樣本量很小，這個結論還不夠強，建議累積更多資料(例如30天)後再決定要不要真的從")
        print("   extract_ml_features()裡刪掉這些特徵——現在就刪，等於用極小樣本的雜訊做決定，風險不小於不刪。")

    # ── 步驟二：贏家 vs 輸家的特徵分布對比 ──
    print("\n=== 步驟二：贏家(win=1) vs 輸家(win=0) 的特徵平均值對比 ===")
    if df["win"].nunique()<2:
        print("⚠️ 這批資料裡贏家或輸家只有一種，沒辦法對比(可能還沒累積到任何虧損或任何獲利的交易)")
    else:
        comparison=df.groupby("win")[ML_FEATURE_NAMES].mean().T
        comparison.columns=["輸家平均","贏家平均"] if list(comparison.columns)==[0,1] else comparison.columns
        comparison["差異"]=comparison.iloc[:,1]-comparison.iloc[:,0] if comparison.shape[1]>=2 else None
        print(comparison.to_string())
        print("\n💡 看「差異」欄位絕對值最大的幾項——這些是「贏家跟輸家在進場那一刻最不一樣的地方」，")
        print("   值得對照步驟一的SHAP排名，兩邊都認為重要的特徵，才是真正有把握的訊號來源。")

    # ── 圖形輸出 ──
    if not args.no_plot:
        try:
            import matplotlib
            shap.summary_plot(shap_values,X,plot_type="bar",show=False)
            import matplotlib.pyplot as plt
            plt.tight_layout(); plt.savefig("shap_summary.png",dpi=150)
            print("\n✅ 圖表已存成 shap_summary.png")
        except Exception as e:
            print(f"\n(圖形輸出失敗，但上面的文字結果不受影響: {e})")

if __name__=="__main__":
    main()
