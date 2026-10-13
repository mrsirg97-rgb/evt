package broadcast

import "testing"

func TestAPostCarriesTheClaimItMirrors(t *testing.T) {
	meshes := meshRoom(t, 2, nil, fullMesh(2))
	startAll(t, meshes)
	act, ok := meshes[0].Post(7, 20)
	if !ok {
		t.Fatal("the member must be alive to post")
	}
	if act.Claim == nil {
		t.Fatalf("a claim act must name the unix-room claim it mirrors, got nil (the mirror's home door cannot map the front's request mint to the act)")
	}
}
