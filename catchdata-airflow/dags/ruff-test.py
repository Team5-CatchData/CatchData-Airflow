# dags/diagnose_connection.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime

def diagnose():
    hook = RedshiftSQLHook(redshift_conn_id='redshift_dev_db')
    
    print("=" * 70)
    print("1. Connection 설정 정보")
    print("=" * 70)
    conn = hook.get_connection('redshift_dev_db')
    print(f"Host: {conn.host}")
    print(f"Port: {conn.port}")
    print(f"Database: {conn.schema}")  # Database 필드값
    print(f"Login: {conn.login}")
    
    print("\n" + "=" * 70)
    print("2. 실제 연결된 정보")
    print("=" * 70)
    result = hook.get_records("""
        SELECT 
            current_database() as db,
            current_user as user,
            version()
    """)
    print(f"실제 Database: {result[0][0]}")
    print(f"실제 User: {result[0][1]}")
    print(f"Redshift Version: {result[0][2][:100]}...")
    
    print("\n" + "=" * 70)
    print("3. 모든 스키마 목록")
    print("=" * 70)
    schemas = hook.get_records("""
        SELECT schema_name 
        FROM information_schema.schemata 
        ORDER BY schema_name
    """)
    print(f"총 {len(schemas)}개 스키마:")
    for schema in schemas:
        print(f"  - {schema[0]}")
    
    print("\n" + "=" * 70)
    print("4. 'jaehyeon' 스키마 검색")
    print("=" * 70)
    jaehyeon_exists = any(s[0] == 'jaehyeon' for s in schemas)
    if jaehyeon_exists:
        print("✓ jaehyeon 스키마 발견!")
        
        # 테이블 확인
        tables = hook.get_records("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'jaehyeon'
        """)
        print(f"  테이블 개수: {len(tables)}")
        for table in tables:
            print(f"    - {table[0]}")
    else:
        print("✗ jaehyeon 스키마 없음!")
        print("\n원인 분석:")
        print("  - Jupyter와 다른 Redshift에 연결되었거나")
        print("  - Jupyter와 다른 Database에 연결됨")
        print("\nJupyter 정보 (비교용):")
        print("  Database: dev")
        print("  Schema: jaehyeon (존재)")
        print("\nAirflow 정보:")
        print(f"  Database: {result[0][0]}")
        print("  Schema: jaehyeon (없음)")
        
    print("\n" + "=" * 70)
    print("5. 모든 테이블 검색 (restaurant_airflow 찾기)")
    print("=" * 70)
    all_tables = hook.get_records("""
        SELECT 
            table_schema, 
            table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%restaurant%'
        ORDER BY table_schema, table_name
    """)
    
    if all_tables:
        print("restaurant 관련 테이블:")
        for schema, table in all_tables:
            print(f"  - {schema}.{table}")
    else:
        print("restaurant 관련 테이블 없음")
    
    print("\n" + "=" * 70)
    print("결론")
    print("=" * 70)
    
    if result[0][0] != 'dev':
        print(f"❌ 문제: Airflow가 '{result[0][0]}' database에 연결됨")
        print("   Jupyter는 'dev' database 사용 중")
        print("\n해결: Connection의 Database 필드를 'dev'로 변경")
    elif not jaehyeon_exists:
        print("❌ 문제: Database는 맞는데 jaehyeon 스키마가 없음")
        print("   Airflow와 Jupyter가 다른 Redshift 클러스터를 보고 있을 가능성")
        print("\n해결: Connection의 Host가 정확한지 다시 확인")
        print(f"   현재 Host: {conn.host}")
    else:
        print("✓ 모든 설정이 정상입니다!")

with DAG(
    'diagnose_connection',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['debug']
) as dag:
    
    PythonOperator(
        task_id='diagnose',
        python_callable=diagnose
    )