package SSTable

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

func buildMerkleTree(data [][]byte) *Node {
	if len(data) == 0 {
		return nil
	}

	var nodes []*Node

	// Create leaf nodes for each data element and hash them individually.
	for _, datum := range data {
		node := &Node{
			data: Hash(datum),
		}
		nodes = append(nodes, node)
	}

	// Build the Merkle tree by combining and hashing pairs of nodes.

	for len(nodes) > 1 {

		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {

			if i+1 < len(nodes) {

				newNode := &Node{
					data:  Hash(append(nodes[i].data, nodes[i+1].data...)),
					left:  nodes[i],
					right: nodes[i+1],
				}

				newLevel = append(newLevel, newNode)

			} else {
				newLevel = append(newLevel, nodes[i])
			}
		}

		nodes = newLevel
	}

	return nodes[0]
}

func serializeMerkleTree(root *Node) []byte {

	if root == nil {
		return nil
	}

	if root.left == nil && root.right == nil {
		return root.data
	}

	leftBytes := serializeMerkleTree(root.left)
	rightBytes := serializeMerkleTree(root.right)

	return append(root.data, append(leftBytes, rightBytes...)...)
}

func main() {
	// Example data elements (in real-world applications, these would be the actual data or transactions).
	data := [][]byte{
		[]byte("Data1"),
		[]byte("Data2"),
		[]byte("Data3"),
		[]byte("Data4"),
	}

	// Create the Merkle tree from the data.
	root := buildMerkleTree(data)

	// Print the Merkle root.
	fmt.Printf("Merkle Root: %x\n", root.data)

	// Serialize the Merkle tree.
	serializedTree := serializeMerkleTree(root)

	// Save the serialized Merkle tree to a file.
	err := ioutil.WriteFile("merkle_tree.dat", serializedTree, 0644)
	if err != nil {
		fmt.Println("Error saving Merkle tree to file:", err)
		return
	}

	fmt.Println("Merkle tree serialized and saved to merkle_tree.dat")

}
