package treap

import (
	"math/rand"
	"time"
)

type Node struct {
	Price    float64
	Quantity float64
	Priority uint32
	Left     *Node
	Right    *Node
}

type Treap struct {
	Root       *Node
	rng        *rand.Rand
	TotalDepth float64
}

func New() *Treap {
	return &Treap{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (t *Treap) Insert(price, quantity float64) {
	oldQty, _ := t.Get(price)
	delta := quantity - oldQty
	t.TotalDepth += delta

	if quantity == 0 {
		t.Root = t.remove(t.Root, price)
		return
	}
	t.Root = t.insert(t.Root, price, quantity)
}

func (t *Treap) Remove(price float64) {
	oldQty, exists := t.Get(price)
	if exists {
		t.TotalDepth -= oldQty
	}
	t.Root = t.remove(t.Root, price)
}

func (t *Treap) Get(price float64) (float64, bool) {
	node := t.find(t.Root, price)
	if node == nil {
		return 0, false
	}
	return node.Quantity, true
}

func (t *Treap) MinPrice() float64 {
	if t.Root == nil {
		return 0
	}
	node := t.Root
	for node.Left != nil {
		node = node.Left
	}
	return node.Price
}

func (t *Treap) MaxPrice() float64 {
	if t.Root == nil {
		return 0
	}
	node := t.Root
	for node.Right != nil {
		node = node.Right
	}
	return node.Price
}

func (t *Treap) SumDepth() float64 {
	return t.TotalDepth
}

func (t *Treap) IsEmpty() bool {
	return t.Root == nil
}

func (t *Treap) insert(node *Node, price, quantity float64) *Node {
	if node == nil {
		return &Node{
			Price:    price,
			Quantity: quantity,
			Priority: t.rng.Uint32(),
		}
	}

	if price < node.Price {
		node.Left = t.insert(node.Left, price, quantity)
		if node.Left.Priority > node.Priority {
			node = t.rotateRight(node)
		}
	} else if price > node.Price {
		node.Right = t.insert(node.Right, price, quantity)
		if node.Right.Priority > node.Priority {
			node = t.rotateLeft(node)
		}
	} else {
		node.Quantity = quantity
	}

	return node
}

func (t *Treap) remove(node *Node, price float64) *Node {
	if node == nil {
		return nil
	}

	if price < node.Price {
		node.Left = t.remove(node.Left, price)
	} else if price > node.Price {
		node.Right = t.remove(node.Right, price)
	} else {
		if node.Left == nil {
			return node.Right
		}
		if node.Right == nil {
			return node.Left
		}
		if node.Left.Priority > node.Right.Priority {
			node = t.rotateRight(node)
			node.Right = t.remove(node.Right, price)
		} else {
			node = t.rotateLeft(node)
			node.Left = t.remove(node.Left, price)
		}
	}

	return node
}

func (t *Treap) find(node *Node, price float64) *Node {
	if node == nil {
		return nil
	}
	if price < node.Price {
		return t.find(node.Left, price)
	}
	if price > node.Price {
		return t.find(node.Right, price)
	}
	return node
}

func (t *Treap) rotateRight(node *Node) *Node {
	newRoot := node.Left
	node.Left = newRoot.Right
	newRoot.Right = node
	return newRoot
}

func (t *Treap) rotateLeft(node *Node) *Node {
	newRoot := node.Right
	node.Right = newRoot.Left
	newRoot.Left = node
	return newRoot
}
