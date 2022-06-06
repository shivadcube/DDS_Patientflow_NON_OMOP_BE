import string
import random


class LOTDrilDown():
    """docstring for LOTDrilDown"""

    def __init__(self):
        self.treated_color = "#ff1573"
        self.level_color_dict = ["#ff1573", "#92d050", "#00beb3", "#ffc000", "#9a457d"]

    def get_random_node_id(self):
        chars_list = '{}{}{}'
        chars_list = chars_list.format(
            string.ascii_uppercase,
            string.digits,
            string.ascii_lowercase
        )
        return ''.join(random.choice(chars_list) for _ in range(10))

    def get_color(self, level_id):
        return self.level_color_dict[level_id % len(self.level_color_dict)]

    def append_dict(self, k, v, summary, node_id, level_id=1):
        if isinstance(v, dict):
            temp_summary = {}
            temp_summary['name'] = k
            keys = list(v.keys())
            if len(keys) <= 1:
                temp_summary['count'] = keys[0]
                temp_summary['level'] = level_id
                temp_summary['nodeId'] = node_id
                temp_summary['color'] = self.get_color(level_id)
                temp_summary['children'] = []
                new_dict = v.get(keys[0])
                level_id = level_id + 1
                temp_summary['children'] = []
                test = {}
                for key, val in new_dict.items():
                    test = self.append_dict(key, val, temp_summary, node_id, level_id=level_id)
                    temp_summary['children'].append(test)
                return temp_summary
            else:
                level_id = level_id + 1
                test = {}
                for key, val in v.items():
                    test = self.append_dict(key, val, summary, node_id, level_id=level_id)
                    summary['children'].append(test)
                return summary
        else:
            temp_summary = {}
            temp_summary['name'] = k
            temp_summary['count'] = v
            temp_summary['level'] = level_id
            temp_summary['nodeId'] = node_id
            temp_summary['color'] = self.get_color(level_id)
            return temp_summary

    def construct_dict(self, dic, node_id):
        output_summary = []
        for k, v in dic.items():
            level_id = 1
            output_summary.append(self.append_dict(k, v, output_summary, node_id, level_id=level_id))
        return output_summary

    def get_lot_json(self, out_dict):
        node_id = self.get_random_node_id()
        final_summary = {}
        loop = 1
        for key, value in out_dict.items():
            if loop == 1:
                final_summary['name'] = key
                final_summary['count'] = value
                final_summary['color'] = self.treated_color
                final_summary['nodeId'] = node_id
                final_summary['level'] = 0
                if 'children' in out_dict.keys():
                    final_summary['children'] = []
            else:
                output = self.construct_dict(value, node_id)
                final_summary['children'] = output
            loop = loop + 1
        return final_summary
