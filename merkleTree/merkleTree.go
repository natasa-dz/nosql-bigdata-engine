package merkleTree

import (
	. "NAiSP/Log"
	"crypto/sha256"
	"encoding/hex"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	Data  []byte
	Left  *Node
	Right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.Data[:])
}

func Hash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

func AppendLog(log *Log, leafNodes *[]*Node) {
	node := &Node{
		Data: append(log.Key, log.Value...), // Concatenate Key and Value for simplicity
	}
	*leafNodes = append(*leafNodes, node)
}

func BuildMerkleTreeCompaction(leafNodes []*Node) *Node {
	// Build the Merkle tree by combining and hashing pairs of nodes.
	for len(leafNodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(leafNodes); i += 2 {
			if i+1 < len(leafNodes) {
				newNode := &Node{
					Data:  Hash(append(leafNodes[i].Data, leafNodes[i+1].Data...)),
					Left:  leafNodes[i],
					Right: leafNodes[i+1],
				}
				newLevel = append(newLevel, newNode)
			} else {
				// If there's an odd number of nodes, simply add the last node to the new level.
				newLevel = append(newLevel, leafNodes[i])
			}
		}

		leafNodes = newLevel
	}

	// The last remaining node is the root of the Merkle tree.
	return leafNodes[0]
}

// bottom-up izgradnja, pretpostavka da imamo key:value parove!!!!!!!!
func BuildMerkleTreeRoot(sortedData []*Log) *Node {
	// Create leaf nodes for each data entry and hash them individually.
	var leafNodes []*Node
	for _, data := range sortedData {
		node := &Node{
			Data: append(data.Key, data.Value...), // Concatenate Key and Value for simplicity
		}
		leafNodes = append(leafNodes, node)
	}

	// Build the Merkle tree by combining and hashing pairs of nodes.
	for len(leafNodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(leafNodes); i += 2 {
			if i+1 < len(leafNodes) {
				newNode := &Node{
					Data:  Hash(append(leafNodes[i].Data, leafNodes[i+1].Data...)),
					Left:  leafNodes[i],
					Right: leafNodes[i+1],
				}
				newLevel = append(newLevel, newNode)
			} else {
				// If there's an odd number of nodes, simply add the last node to the new level.
				newLevel = append(newLevel, leafNodes[i])
			}
		}

		leafNodes = newLevel
	}

	// The last remaining node is the root of the Merkle tree.
	return leafNodes[0]
}

func SerializeMerkleTree(root *Node) []byte {

	if root == nil {
		return nil
	}

	if root.Left == nil && root.Right == nil {
		return root.Data
	}

	leftBytes := SerializeMerkleTree(root.Left)
	rightBytes := SerializeMerkleTree(root.Right)

	return append(root.Data, append(leftBytes, rightBytes...)...)
}

// FJA KORISCENA ZA TESTOVE U MAINU OVDE(OSTAVITI ZBOG TESTIRANJA AKO BUDE BAGOVA) SEM TOGA NEPOTREBNA
func buildMerkleTree(data [][]byte) *Node {
	if len(data) == 0 {
		return nil
	}

	var nodes []*Node

	// Create leaf nodes for each Data element and hash them individually.
	for _, datum := range data {
		node := &Node{
			Data: Hash(datum),
		}
		nodes = append(nodes, node)
	}

	// Build the Merkle tree by combining and hashing pairs of nodes.

	for len(nodes) > 1 {

		var newLevel []*Node

		for i := 0; i < len(nodes); i += 2 {

			if i+1 < len(nodes) {

				newNode := &Node{
					Data:  Hash(append(nodes[i].Data, nodes[i+1].Data...)),
					Left:  nodes[i],
					Right: nodes[i+1],
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

//func main() {
//	// Example Data elements (in real-world applications, these would be the actual Data or transactions).
//	data := [][]byte{
//		[]byte("Data1"),
//		[]byte("Data2"),
//		[]byte("Data3"),
//		[]byte("Data4"),
//	}
//
//	// Create the Merkle tree from the Data.
//	root := buildMerkleTree(data)
//
//	// Print the Merkle root.
//	fmt.Printf("Merkle Root: %x\n", root.Data)
//
//	// Serialize the Merkle tree.
//	serializedTree := SerializeMerkleTree(root)
//
//	// Save the serialized Merkle tree to a file.
//	err := ioutil.WriteFile("merkle_tree.dat", serializedTree, 0644)
//	if err != nil {
//		fmt.Println("Error saving Merkle tree to file:", err)
//		return
//	}
//
//	fmt.Println("Merkle tree serialized and saved to merkle_tree.dat")
//
//}
