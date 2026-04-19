import pandas as pd
import os
import glob

def total_preprocessing():
    curr_path = os.path.dirname(os.path.abspath(__file__))
    print("-" * 50)
    print(f"🚀 [디코딩 수정] 데이터 타입 일치화 및 최종 통합 시작")

    try:
        def smart_read(keyword):
            files = glob.glob(os.path.join(curr_path, f"*{keyword}*"))
            if not files: return None
            target = files[0]
            for enc in ['utf-8-sig', 'utf-8', 'cp949']:
                try:
                    df = pd.read_csv(target, encoding=enc)
                    df.columns = df.columns.str.strip().str.upper()
                    return df
                except: continue
            return None

        # 1. 파일 로드
        df_master = smart_read("traveller_master")
        df_travel = smart_read("travel_여")
        df_activity = smart_read("activity_his")
        df_companion = smart_read("companion_info")
        df_adv = smart_read("adv_consume_his")
        df_visit = smart_read("visit_area_info")
        df_code_b = smart_read("tc_codeb_코드B")

        if all(df is not None for df in [df_master, df_travel, df_activity, df_code_b]):
            
            # [Step 1] 기본 통합
            base_df = pd.merge(df_master[['TRAVELER_ID', 'GENDER', 'AGE_GRP']], 
                               df_travel[['TRAVELER_ID', 'TRAVEL_ID']], on='TRAVELER_ID', how='inner')

            if df_companion is not None:
                comp_sub = df_companion[['TRAVEL_ID', 'REL_CD', 'COMPANION_AGE_GRP']].drop_duplicates(['TRAVEL_ID'])
                base_df = pd.merge(base_df, comp_sub, on='TRAVEL_ID', how='left')

            # [Step 2] ⭐ 디코딩 수정 (데이터 타입 강제 일치)
            # 코드북의 CD_B를 문자로 변환
            df_code_b['CD_B'] = df_code_b['CD_B'].astype(str).str.strip()
            tcr_map = df_code_b[df_code_b['CD_A'] == 'TCR'].set_index('CD_B')['CD_NM'].to_dict()
            
            # 데이터의 REL_CD를 문자로 변환 (소수점 제거 후 문자로)
            def convert_to_str(val):
                if pd.isna(val): return None
                return str(int(float(val)))

            base_df['REL_NM'] = base_df['REL_CD'].apply(convert_to_str).map(tcr_map)

            # [Step 3] 나머지 통합
            if df_adv is not None:
                adv_sub = df_adv[['TRAVEL_ID', 'ADV_NM', 'PAYMENT_AMT_WON']].drop_duplicates(['TRAVEL_ID'])
                base_df = pd.merge(base_df, adv_sub, on='TRAVEL_ID', how='left')
            if df_visit is not None:
                visit_sub = df_visit[['TRAVEL_ID', 'VISIT_AREA_NM', 'RESIDENCE_TIME_MIN']].drop_duplicates(['TRAVEL_ID'])
                base_df = pd.merge(base_df, visit_sub, on='TRAVEL_ID', how='left')

            act_col = 'ACTIVITY_DTL' if 'ACTIVITY_DTL' in df_activity.columns else df_activity.columns[4]
            final_df = pd.merge(base_df, df_activity[['TRAVEL_ID', act_col]], on='TRAVEL_ID', how='inner')

            # 4. 결과 저장
            final_df.to_csv('final_travel_data_decoded.csv', index=False, encoding='utf-8-sig')
            
            print(f"✨ 이번에는 진짜 성공! 'REL_NM'에 한글이 들어갔는지 확인하세요.")
            # 한글이 들어간 것만 골라서 출력해봅니다.
            check_df = final_df[final_df['REL_NM'].notnull()]
            if not check_df.empty:
                print(check_df[['TRAVEL_ID', 'REL_CD', 'REL_NM']].head(10))
            else:
                print("⚠️ 여전히 매칭되는 데이터가 없습니다. ID 범위를 확인해야 합니다.")
            
            return final_df

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    total_preprocessing()