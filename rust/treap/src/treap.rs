use std::cmp::Ordering;

struct TreapNodePtr<K: Ord, P: Ord, V>(Option<Box<TreapNode<K, P, V>>>);

struct TreapNode<K: Ord, P: Ord, V> {
    key: K,
    priority: P,
    value: V,
    left: TreapNodePtr<K, P, V>,
    right: TreapNodePtr<K, P, V>,
}

type Treap<K, P, V> = TreapNodePtr<K, P, V>;

impl<K: Ord, P: Ord, V> From<Box<TreapNode<K, P, V>>> for TreapNodePtr<K, P, V> {
    fn from(node: Box<TreapNode<K, P, V>>) -> Self {
        TreapNodePtr(Some(node))
    }
}

impl<K: Ord, P: Ord, V> TreapNodePtr<K, P, V> {
    pub fn default() -> Self {
        TreapNodePtr(None)
    }

    fn new(key: K, priority: P, value: V) -> Self {
        TreapNodePtr(Some(Box::from(TreapNode {
            key,
            priority,
            value,
            left: TreapNodePtr(None),
            right: TreapNodePtr(None),
        })))
    }

    fn take(&mut self) -> Self {
        TreapNodePtr(self.0.take())
    }

    fn split<F>(self, pred: F) -> (Self, Self)
    where
        F: Fn(&K) -> bool,
    {
        if let Some(mut node) = self.0 {
            let left = node.left.take();
            let right = node.right.take();
            if pred(&node.key) {
                let (right_l, right_r) = right.split(pred);
                node.left = left;
                node.right = right_l;
                (TreapNodePtr(Some(node)), right_r)
            } else {
                let (left_l, left_r) = left.split(pred);
                node.left = left_r;
                node.right = right;
                (left_l, TreapNodePtr(Some(node)))
            }
        } else {
            (TreapNodePtr(None), TreapNodePtr(None))
        }
    }

    fn split_by_key(self, key: &K) -> (Self, Option<(K, V)>, Self) {
        if let Some(mut node) = self.0 {
            let left = node.left.take();
            let right = node.right.take();
            if node.key == *key {
                (left, Some((node.key, node.value)), right)
            } else if node.key < *key {
                let (right_l, elem, right_r) = right.split_by_key(key);
                node.left = left;
                node.right = right_l;
                (TreapNodePtr(Some(node)), elem, right_r)
            } else {
                let (left_l, elem, left_r) = left.split_by_key(key);
                node.left = left_r;
                node.right = right;
                (left_l, elem, TreapNodePtr(Some(node)))
            }
        } else {
            (TreapNodePtr(None), None, TreapNodePtr(None))
        }
    }

    fn merge(left: Self, right: Self) -> Self {
        let mut left_node: Box<TreapNode<K, P, V>>;
        let mut right_node: Box<TreapNode<K, P, V>>;

        match left.0 {
            None => return right,
            Some(node) => left_node = node,
        }

        match right.0 {
            None => return TreapNodePtr(Some(left_node)),
            Some(node) => right_node = node,
        }

        if left_node.priority >= right_node.priority {
            let left_r = left_node.right.take();
            left_node.right = TreapNodePtr::merge(left_r, TreapNodePtr::from(right_node));
            TreapNodePtr::from(left_node)
        } else {
            let right_l = right_node.left.take();
            right_node.left = TreapNodePtr::merge(TreapNodePtr::from(left_node), right_l);
            TreapNodePtr::from(right_node)
        }
    }

    pub fn insert(&mut self, k: K, p: P, v: V) -> Option<(K, V)> {
        let root = self.take();
        let (left, elem, right) = root.split_by_key(&k);
        let new_node = TreapNodePtr::new(k, p, v);
        *self = TreapNodePtr::merge(left, TreapNodePtr::merge(new_node, right));
        elem
    }

    pub fn erase(&mut self, key: &K) -> Option<(K, V)> {
        let root = self.take();
        let (left, elem, right) = root.split_by_key(key);
        *self = TreapNodePtr::merge(left, right);
        elem
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.0
            .as_ref()
            .map_or(None, |node| match node.key.cmp(key) {
                Ordering::Equal => Some(&node.value),
                Ordering::Less => node.right.get(key),
                Ordering::Greater => node.left.get(key),
            })
    }

    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn peek(&self) -> Option<(&K, &V)> {
        self.0.as_ref().map(|node| (&node.key, &node.value))
    }

    pub fn pop(&mut self) -> Option<(K, V)> {
        let root = self.take();
        match root.0 {
            None => None,
            Some(mut node) => {
                let left = node.left.take();
                let right = node.right.take();
                *self = TreapNodePtr::merge(left, right);
                Some((node.key, node.value))
            }
        }
    }

    pub fn into_iter_by_priority(self) -> IterByPriority<K, P, V> {
        IterByPriority { treap: self }
    }

    fn collect(self, vec: &mut Vec<(K, V)>) {
        if let Some(mut node) = self.0 {
            let right = node.right.take();
            node.left.collect(vec);
            vec.push((node.key, node.value));
            right.collect(vec)
        }
    }

    pub fn into_vec(self) -> Vec<(K, V)> {
        let mut vec = Vec::new();
        self.collect(&mut vec);
        vec
    }
}

struct IterByPriority<K: Ord, P: Ord, V> {
    treap: Treap<K, P, V>,
}

impl<K: Ord, P: Ord, V> Iterator for IterByPriority<K, P, V> {
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.treap.pop()
    }
}

#[cfg(test)]
mod tests {
    use crate::treap::Treap;
    #[test]
    fn it_works() {
        let mut treap: Treap<&str, i64, &str> = Treap::default();
        assert_eq!(None, treap.insert("k4", 3, "v4"));
        assert_eq!(None, treap.insert("k1", 5, "v1"));
        assert_eq!(None, treap.insert("k2", 4, "v2"));
        assert_eq!(None, treap.insert("k3", 1, "v3"));

        assert_eq!("v1", *treap.get(&"k1").unwrap());

        assert_eq!(Some(("k1", "v1")), treap.erase(&"k1"));
        assert_eq!(None, treap.get(&"k1"));

        assert_eq!(Some(("k2", "v2")), treap.pop());
        assert_eq!(Some((&"k4", &"v4")), treap.peek());
        assert_eq!(None, treap.get(&"k2"));
        assert_eq!(Some(&"v3"), treap.get(&"k3"));
    }
}
