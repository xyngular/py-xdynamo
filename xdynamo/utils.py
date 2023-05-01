
def _all_possible_query_names_for_hash_key_name(hash_key_name):
    for operator in ('', '__in', '__exact', '__eq'):
        yield f'{hash_key_name}{operator}'
