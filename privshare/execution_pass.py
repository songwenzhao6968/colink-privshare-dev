from privshare import myutil
from privshare.execution import AndNode, OrNode, NotNode, MatchBitsNode, NodeType

class Pass:
    # Pre-expansion optimization
    @staticmethod
    def merge_range(exe_tree):
        """
        Merge the following:
            intersect and: [1, 5] and [3, 8] -> [3, 5]
            intersect or: [5, 7] or [2, 6] -> [2, 7]
            or with two edges: [min, 4] or [7, max] -> not [5, 6]
            (stupid queries like "[1, 3] and [6, 8]" are not considered at all)
        """
        def rewrite_node(node):
            if node.type == NodeType.AND:
                left_child = rewrite_node(node.children[0])
                right_child = rewrite_node(node.children[1])
                if (left_child.type == NodeType.RANGE and right_child.type == NodeType.RANGE and 
                    left_child.concerned_column == right_child.concerned_column and
                    left_child.value_r >= right_child.value_l and right_child.value_r >= left_child.value_l):
                    left_child.value_l = max(left_child.value_l, right_child.value_l)
                    left_child.value_r = min(left_child.value_r, right_child.value_r)
                    return left_child
                return node
            elif node.type == NodeType.OR:
                left_child = rewrite_node(node.children[0])
                right_child = rewrite_node(node.children[1])
                if (left_child.type == NodeType.RANGE and right_child.type == NodeType.RANGE and
                    left_child.concerned_column == right_child.concerned_column):
                    if left_child.value_r >= right_child.value_l - 1 and right_child.value_r >= left_child.value_l - 1: # Note that [5,8] or [9,12] -> [5,12] is also okay
                        left_child.value_l = min(left_child.value_l, right_child.value_l)
                        left_child.value_r = max(left_child.value_r, right_child.value_r)
                        return left_child
                    if left_child.need_int_to_uint_conversion:
                        _min = myutil.int_to_uint(-1 << (left_child.bit_width - 1))
                        _max = myutil.int_to_uint((1 << (left_child.bit_width - 1)) - 1)
                    else:
                        _min, _max = 0, (1 << left_child.bit_width) - 1
                    if ((left_child.value_l == _min and right_child.value_r == _max) or
                        (right_child.value_l == _min and left_child.value_r == _max)):
                        node_not = NotNode()
                        value_l = min(left_child.value_r, right_child.value_r) + 1
                        value_r = max(left_child.value_l, right_child.value_l) - 1
                        left_child.value_l, left_child.value_r = value_l, value_r
                        node_not.link(left_child)
                        return node_not

            children = node.children
            node.children = []
            for child in children:
                node.link(rewrite_node(child))
            return node
    
        exe_tree.root = rewrite_node(exe_tree.root)
        return exe_tree
        
    # Later: merge_equal

    @staticmethod
    def remove_or(exe_tree):
        def rewrite_node(node):
            if node.type == NodeType.OR:
                node_not0, node_not1, node_not2 = NotNode(), NotNode(), NotNode()
                node_and = AndNode()
                node_not0.link(node_and)
                node_and.link(node_not1); node_and.link(node_not2)
                node_not1.link(rewrite_node(node.children[0]))
                node_not2.link(rewrite_node(node.children[1]))
                return node_not0
            children = node.children
            node.children = []
            for child in children:
                node.link(rewrite_node(child))
            return node
        
        exe_tree.root = rewrite_node(exe_tree.root)
        return exe_tree
    
    @staticmethod
    def decompose_equal(exe_tree):
        def rewrite_node(node):
            if node.type == NodeType.EQUAL:
                if node.bit_width == 8:
                    node_mb = MatchBitsNode.decompose_equal(node, 0)
                    return node_mb
                elif node.bit_width == 16:
                    node_mb0 = MatchBitsNode.decompose_equal(node, 0)
                    node_mb1 = MatchBitsNode.decompose_equal(node, 8)
                    node_and = AndNode()
                    node_and.link(node_mb0); node_and.link(node_mb1)
                    return node_and
                elif node.bit_width == 32:
                    node_mb0 = MatchBitsNode.decompose_equal(node, 0)
                    node_mb1 = MatchBitsNode.decompose_equal(node, 8)
                    node_mb2 = MatchBitsNode.decompose_equal(node, 16)
                    node_mb3 = MatchBitsNode.decompose_equal(node, 24)
                    node_and0, node_and1, node_and2 = AndNode(), AndNode(), AndNode()
                    node_and0.link(node_and1); node_and0.link(node_and2)
                    node_and1.link(node_mb0); node_and1.link(node_mb1)
                    node_and2.link(node_mb2); node_and2.link(node_mb3)
                    return node_and0
            children = node.children
            node.children = []
            for child in children:
                node.link(rewrite_node(child))
            return node
        
        exe_tree.root = rewrite_node(exe_tree.root)
        return exe_tree

    @staticmethod
    def decompose_range(exe_tree):
        def rewrite_node(node):
            if node.type == NodeType.RANGE:
                if node.bit_width == 8:
                    node_mb = MatchBitsNode.decompose_range(node, 0, False, True, True)
                    return node_mb
                elif node.bit_width == 16:
                    _min, _max = 0, (1 << node.bit_width) - 1
                    if node.value_l == _min: # Right-side  
                        node_mb0 = MatchBitsNode.decompose_range(node, 8, False, False, True, False, True)
                        node_mb1 = MatchBitsNode.decompose_range(node, 8, True, False, True)
                        node_mb2 = MatchBitsNode.decompose_range(node, 0, False, False, True, False, False)
                        node_and = AndNode()
                        node_or = OrNode()
                        node_and.link(node_mb1); node_and.link(node_mb2)
                        node_or.link(node_and); node_or.link(node_mb0)
                        return node_or
                    elif node.value_r == _max: # Left-side
                        node_mb0 = MatchBitsNode.decompose_range(node, 8, False, True, False, True, False)
                        node_mb1 = MatchBitsNode.decompose_range(node, 8, True, True, False)
                        node_mb2 = MatchBitsNode.decompose_range(node, 0, False, True, False, False, False)
                        node_and = AndNode()
                        node_or = OrNode()
                        node_and.link(node_mb1); node_and.link(node_mb2)
                        node_or.link(node_and); node_or.link(node_mb0)
                        return node_or
                    else:
                        raise NotImplementedError # Double-side
                elif node.bit_width == 32:
                    raise NotImplementedError
            children = node.children
            node.children = []
            for child in children:
                node.link(rewrite_node(child))
            return node
        
        exe_tree.root = rewrite_node(exe_tree.root)
        return exe_tree
    
    # Later: post-expansion optimization