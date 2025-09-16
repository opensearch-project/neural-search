#!/usr/bin/env python3

contexts = {
    "Test 1 - Robotics": {
        "context": "Our school offers a comprehensive education rooted in Christian values. We have state-of-the-art facilities including science labs and computer rooms. The robotics program is one of our most popular extracurricular activities, winning multiple regional competitions. Students learn programming, engineering, and teamwork skills. Faith and academic excellence go hand in hand at our institution.",
        "highlights": [(151, 266)]
    },
    "Test 2 - Autism": {
        "context": "Welcome to our inclusive educational community. Christian teachings form the foundation of our curriculum. We pride ourselves on supporting diverse learners. Our specialized autism support program provides individualized instruction and therapeutic services. Trained specialists work closely with students on the autism spectrum. We believe every child deserves quality education in a nurturing environment.",
        "highlights": [(158, 258)]
    },
    "Test 3 - Special Programs": {
        "context": "Our institution emphasizes traditional values and modern education. The campus features beautiful grounds and historic buildings. We offer specialized programs including advanced robotics, autism support services, and gifted education. Each program is staffed by certified specialists. Our commitment to excellence extends to all areas of student life.",
        "highlights": [(130, 235), (236, 285)]
    }
}

print("="*60)
print("SEMANTIC HIGHLIGHTING RESULTS ANALYSIS")
print("="*60)

for test_name, data in contexts.items():
    print(f"\n{test_name}:")
    print("-" * 40)
    sentences = data["context"].split(". ")

    # Show sentence breakdown
    print("Sentences in context:")
    for i, sent in enumerate(sentences, 1):
        if sent:  # Skip empty strings
            print(f"  {i}. {sent}{'.' if not sent.endswith('.') else ''}")

    print("\nHighlighted text:")
    for start, end in data["highlights"]:
        highlighted_text = data["context"][start:end]
        print(f"  [{start}:{end}] \"{highlighted_text}\"")

    # Identify which sentences were highlighted
    print("\nSentence analysis:")
    for start, end in data["highlights"]:
        highlighted_text = data["context"][start:end]
        for i, sent in enumerate(sentences, 1):
            if sent and sent in highlighted_text:
                print(f"  - Sentence {i} is highlighted")

print("\n" + "="*60)
print("OBSERVATIONS:")
print("="*60)
print("✓ Test 1 (Robotics): CORRECTLY highlights the robotics program sentence")
print("✓ Test 2 (Autism): CORRECTLY highlights the autism support program sentence")
print("✓ Test 3 (Special Programs): CORRECTLY highlights the programs sentence")
print("\nCONTRARY to the forum post, the model IS working correctly here!")
print("It's accurately identifying and highlighting the relevant sentences.")
print("="*60)